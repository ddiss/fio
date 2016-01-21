/*
 * rbd engine
 *
 * IO engine using Ceph's librbd to test RADOS Block Devices.
 *
 */

#include <rbd/librbd.h>

#include "../fio.h"
#include "../optgroup.h"

struct fio_rbd_iou {
	struct io_u *io_u;
	rbd_completion_t completion;
	int io_complete;
};

struct rbd_data {
	rados_t cluster;
	rados_ioctx_t io_ctx;
	rbd_image_t image;
};

struct rbd_options {
	void *pad;
	char *rbd_name;
	char *pool_name;
	char *client_name;
	int busy_poll;
};

static struct fio_option options[] = {
	{
		.name		= "rbdname",
		.lname		= "rbd engine rbdname",
		.type		= FIO_OPT_STR_STORE,
		.help		= "RBD name for RBD engine",
		.off1		= offsetof(struct rbd_options, rbd_name),
		.category	= FIO_OPT_C_ENGINE,
		.group		= FIO_OPT_G_RBD,
	},
	{
		.name		= "pool",
		.lname		= "rbd engine pool",
		.type		= FIO_OPT_STR_STORE,
		.help		= "Name of the pool hosting the RBD for the RBD engine",
		.off1		= offsetof(struct rbd_options, pool_name),
		.category	= FIO_OPT_C_ENGINE,
		.group		= FIO_OPT_G_RBD,
	},
	{
		.name		= "clientname",
		.lname		= "rbd engine clientname",
		.type		= FIO_OPT_STR_STORE,
		.help		= "Name of the ceph client to access the RBD for the RBD engine",
		.off1		= offsetof(struct rbd_options, client_name),
		.category	= FIO_OPT_C_ENGINE,
		.group		= FIO_OPT_G_RBD,
	},
	{
		.name		= "busy_poll",
		.lname		= "Busy poll",
		.type		= FIO_OPT_BOOL,
		.help		= "Busy poll for completions instead of sleeping",
		.off1		= offsetof(struct rbd_options, busy_poll),
		.def		= "0",
		.category	= FIO_OPT_C_ENGINE,
		.group		= FIO_OPT_G_RBD,
	},
	{
		.name = NULL,
	},
};

static int _fio_setup_rbd_data(struct thread_data *td,
			       struct rbd_data **rbd_data_ptr)
{
	struct rbd_data *rbd;

	if (td->io_ops->data)
		return 0;

	rbd = calloc(1, sizeof(struct rbd_data));
	if (!rbd)
		goto failed;

	*rbd_data_ptr = rbd;
	return 0;

failed:
	if (rbd)
		free(rbd);
	return 1;

}

static int _fio_rbd_connect(struct thread_data *td)
{
	struct rbd_data *rbd = td->io_ops->data;
	struct rbd_options *o = td->eo;
	int r;

	r = rados_create(&rbd->cluster, o->client_name);
	if (r < 0) {
		log_err("rados_create failed.\n");
		goto failed_early;
	}

	r = rados_conf_read_file(rbd->cluster, NULL);
	if (r < 0) {
		log_err("rados_conf_read_file failed.\n");
		goto failed_early;
	}

	r = rados_connect(rbd->cluster);
	if (r < 0) {
		log_err("rados_connect failed.\n");
		goto failed_shutdown;
	}

	r = rados_ioctx_create(rbd->cluster, o->pool_name, &rbd->io_ctx);
	if (r < 0) {
		log_err("rados_ioctx_create failed.\n");
		goto failed_shutdown;
	}

	r = rbd_open(rbd->io_ctx, o->rbd_name, &rbd->image, NULL /*snap */ );
	if (r < 0) {
		log_err("rbd_open failed.\n");
		goto failed_open;
	}
	return 0;

failed_open:
	rados_ioctx_destroy(rbd->io_ctx);
	rbd->io_ctx = NULL;
failed_shutdown:
	rados_shutdown(rbd->cluster);
	rbd->cluster = NULL;
failed_early:
	return 1;
}

static void _fio_rbd_disconnect(struct rbd_data *rbd)
{
	if (!rbd)
		return;

	/* shutdown everything */
	if (rbd->image) {
		rbd_close(rbd->image);
		rbd->image = NULL;
	}

	if (rbd->io_ctx) {
		rados_ioctx_destroy(rbd->io_ctx);
		rbd->io_ctx = NULL;
	}

	if (rbd->cluster) {
		rados_shutdown(rbd->cluster);
		rbd->cluster = NULL;
	}
}

static void _fio_rbd_finish_aiocb(rbd_completion_t comp, void *data)
{
	struct fio_rbd_iou *fri = data;
	struct io_u *io_u = fri->io_u;
	ssize_t ret;

	/*
	 * Looks like return value is 0 for success, or < 0 for
	 * a specific error. So we have to assume that it can't do
	 * partial completions.
	 */
	ret = rbd_aio_get_return_value(fri->completion);
	if (ret < 0) {
		io_u->error = ret;
		io_u->resid = io_u->xfer_buflen;
	} else
		io_u->error = 0;

	fri->io_complete = 1;
}

static void rbd_io_u_wait_complete(struct io_u *io_u)
{
	struct fio_rbd_iou *fri = io_u->engine_data;

	rbd_aio_wait_for_complete(fri->completion);
}

static int fio_rbd_queue(struct thread_data *td, struct io_u *io_u)
{
	struct rbd_data *rbd = td->io_ops->data;
	struct fio_rbd_iou *fri = io_u->engine_data;
	int r = -1;

	fio_ro_check(td, io_u);

	fri->io_complete = 0;

	r = rbd_aio_create_completion(fri, _fio_rbd_finish_aiocb,
						&fri->completion);
	if (r < 0) {
		log_err("rbd_aio_create_completion failed.\n");
		goto failed;
	}

	if (io_u->ddir == DDIR_WRITE) {
		r = rbd_aio_write(rbd->image, io_u->offset, io_u->xfer_buflen,
					 io_u->xfer_buf, fri->completion);
		if (r < 0) {
			log_err("rbd_aio_write failed.\n");
			goto failed_comp;
		}

	} else if (io_u->ddir == DDIR_READ) {
		r = rbd_aio_read(rbd->image, io_u->offset, io_u->xfer_buflen,
					io_u->xfer_buf, fri->completion);

		if (r < 0) {
			log_err("rbd_aio_read failed.\n");
			goto failed_comp;
		}
	} else if (io_u->ddir == DDIR_TRIM) {
		r = rbd_aio_discard(rbd->image, io_u->offset,
					io_u->xfer_buflen, fri->completion);
		if (r < 0) {
			log_err("rbd_aio_discard failed.\n");
			goto failed_comp;
		}
	} else if (io_u->ddir == DDIR_SYNC) {
		r = rbd_aio_flush(rbd->image, fri->completion);
		if (r < 0) {
			log_err("rbd_flush failed.\n");
			goto failed_comp;
		}
	} else {
		dprint(FD_IO, "%s: Warning: unhandled ddir: %d\n", __func__,
		       io_u->ddir);
		goto failed_comp;
	}

	rbd_io_u_wait_complete(io_u);
	rbd_aio_release(fri->completion);

	return FIO_Q_COMPLETED;
failed_comp:
	rbd_aio_release(fri->completion);
failed:
	io_u->error = r;
	td_verror(td, io_u->error, "xfer");
	return FIO_Q_COMPLETED;
}

static int fio_rbd_init(struct thread_data *td)
{
	int r;

	r = _fio_rbd_connect(td);
	if (r) {
		log_err("fio_rbd_connect failed, return code: %d .\n", r);
		goto failed;
	}

	return 0;

failed:
	return 1;
}

static void fio_rbd_cleanup(struct thread_data *td)
{
	struct rbd_data *rbd = td->io_ops->data;

	if (rbd) {
		_fio_rbd_disconnect(rbd);
		free(rbd);
	}
}

static int fio_rbd_setup(struct thread_data *td)
{
	rbd_image_info_t info;
	struct fio_file *f;
	struct rbd_data *rbd = NULL;
	int major, minor, extra;
	int r;

	/* log version of librbd. No cluster connection required. */
	rbd_version(&major, &minor, &extra);
	log_info("rbd engine: RBD version: %d.%d.%d\n", major, minor, extra);

	/* allocate engine specific structure to deal with librbd. */
	r = _fio_setup_rbd_data(td, &rbd);
	if (r) {
		log_err("fio_setup_rbd_data failed.\n");
		goto cleanup;
	}
	td->io_ops->data = rbd;

	/* librbd does not allow us to run first in the main thread and later
	 * in a fork child. It needs to be the same process context all the
	 * time. 
	 */
	td->o.use_thread = 1;

	/* connect in the main thread to determine to determine
	 * the size of the given RADOS block device. And disconnect
	 * later on.
	 */
	r = _fio_rbd_connect(td);
	if (r) {
		log_err("fio_rbd_connect failed.\n");
		goto cleanup;
	}

	/* get size of the RADOS block device */
	r = rbd_stat(rbd->image, &info, sizeof(info));
	if (r < 0) {
		log_err("rbd_status failed.\n");
		goto disconnect;
	}
	dprint(FD_IO, "rbd-engine: image size: %lu\n", info.size);

	/* taken from "net" engine. Pretend we deal with files,
	 * even if we do not have any ideas about files.
	 * The size of the RBD is set instead of a artificial file.
	 */
	if (!td->files_index) {
		add_file(td, td->o.filename ? : "rbd", 0, 0);
		td->o.nr_files = td->o.nr_files ? : 1;
		td->o.open_files++;
	}
	f = td->files[0];
	f->real_file_size = info.size;

	/* disconnect, then we were only connected to determine
	 * the size of the RBD.
	 */
	_fio_rbd_disconnect(rbd);
	return 0;

disconnect:
	_fio_rbd_disconnect(rbd);
cleanup:
	fio_rbd_cleanup(td);
	return r;
}

static int fio_rbd_open(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int fio_rbd_invalidate(struct thread_data *td, struct fio_file *f)
{
#if defined(CONFIG_RBD_INVAL)
	struct rbd_data *rbd = td->io_ops->data;

	return rbd_invalidate_cache(rbd->image);
#else
	return 0;
#endif
}

static void fio_rbd_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct fio_rbd_iou *fri = io_u->engine_data;

	if (fri) {
		io_u->engine_data = NULL;
		free(fri);
	}
}

static int fio_rbd_io_u_init(struct thread_data *td, struct io_u *io_u)
{
	struct fio_rbd_iou *fri;

	fri = calloc(1, sizeof(*fri));
	fri->io_u = io_u;
	io_u->engine_data = fri;
	return 0;
}

static struct ioengine_ops ioengine = {
	.name			= "rbd",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_rbd_setup,
	.init			= fio_rbd_init,
	.queue			= fio_rbd_queue,
	.flags			= FIO_SYNCIO,
	.cleanup		= fio_rbd_cleanup,
	.open_file		= fio_rbd_open,
	.invalidate		= fio_rbd_invalidate,
	.options		= options,
	.io_u_init		= fio_rbd_io_u_init,
	.io_u_free		= fio_rbd_io_u_free,
	.option_struct_size	= sizeof(struct rbd_options),
};

static void fio_init fio_rbd_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_rbd_unregister(void)
{
	unregister_ioengine(&ioengine);
}
