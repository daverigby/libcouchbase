/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011, 2012 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/**
 * libcouchbase_create_libev_io_opts() allows you to create an instance
 * of the ioopts that will utilize libev. You may either supply an event
 * loop (if you'd like to add your own events into the loop), or it will
 * create it's own.
 *
 * @author Sergey Avseyev
 */
#ifndef LIBCOUCHBASE_LIBEV_IO_OPTS_H
#define LIBCOUCHBASE_LIBEV_IO_OPTS_H 1

#include <libcouchbase/couchbase.h>
#include <ev.h>

#ifdef __cplusplus
extern "C" {
#endif

    /**
     * Create an instance of an event handler that utilize libev for
     * event notification.
     *
     * @param loop the event loop to hook use (please note that you shouldn't
     *             reference the event loop from multiple threads)
     * @return a pointer to a newly created and initialized event handler
     */
    LIBCOUCHBASE_API
    struct lcb_io_opt_st *lcb_create_libev_io_opts(struct ev_loop *loop);

#ifdef __cplusplus
}
#endif

#endif
