// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package etcd3topo

import (
	"fmt"
	"path"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"

	"github.com/tiglabs/baudengine/topo"
	"strings"
)

// Watch node path
func (s *Server) Watch(ctx context.Context, cell, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return &topo.WatchData{Err: fmt.Errorf("Watch cannot get cell: %v", err)}, nil, nil
	}
	nodePath := path.Join(c.root, filePath)

	// Get the initial version of the file
	initial, err := c.cli.Get(ctx, nodePath)
	if err != nil {
		// Generic error.
		return &topo.WatchData{Err: convertError(err)}, nil, nil
	}
	if len(initial.Kvs) != 1 {
		// Node doesn't exist.
		return &topo.WatchData{Err: topo.ErrNoNode}, nil, nil
	}
	wd := &topo.WatchData{
		Contents: initial.Kvs[0].Value,
		Version:  EtcdVersion(initial.Kvs[0].ModRevision),
	}

	// Create a context, will be used to cancel the watch.
	watchCtx, watchCancel := context.WithCancel(context.Background())

	// Create the Watcher.  We start watching from the response we
	// got, not from the file original version, as the server may
	// not have that much history.
	watcher := c.cli.Watch(watchCtx, nodePath, clientv3.WithRev(initial.Header.Revision))
	if watcher == nil {
		return &topo.WatchData{Err: fmt.Errorf("Watch failed")}, nil, nil
	}

	// Create the notifications channel, send updates to it.
	notifications := make(chan *topo.WatchData, 10)
	go func() {
		defer close(notifications)

		for {
			select {
			case <-watchCtx.Done():
				// This includes context cancelation errors.
				notifications <- &topo.WatchData{
					Err: convertError(watchCtx.Err()),
				}
				return
			case wresp := <-watcher:
				if wresp.Canceled {
					// Final notification.
					notifications <- &topo.WatchData{
						Err: convertError(wresp.Err()),
					}
					return
				}

				for _, ev := range wresp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						notifications <- &topo.WatchData{
							Contents: ev.Kv.Value,
							Version:  EtcdVersion(ev.Kv.Version),
						}
					case mvccpb.DELETE:
						// Node is gone, send a final notice.
						notifications <- &topo.WatchData{
							Err: topo.ErrNoNode,
						}
						return
					default:
						notifications <- &topo.WatchData{
							Err: fmt.Errorf("unexpected event received: %v", ev),
						}
						return
					}
				}
			}
		}
	}()

	return wd, notifications, topo.CancelFunc(watchCancel)
}

func (s *Server) WatchDir(ctx context.Context, cell, dirPath string, version topo.Version) (<-chan *topo.WatchData, topo.CancelFunc, error) {
	c, err := s.clientForCell(ctx, cell)
	if err != nil {
		return nil, nil, fmt.Errorf("Watch cannot get cell: %v", err)
	}

	nodePath := path.Join(c.root, dirPath)
	watchCtx, watchCancel := context.WithCancel(context.Background())

	options := make([]clientv3.OpOption, 0)
	options = append(options, clientv3.WithPrefix())
	if version != nil {
		options = append(options, clientv3.WithRev(int64(version.(EtcdVersion))))
	} else {
		options = append(options, clientv3.WithRev(0))
	}
	watcher := c.cli.Watch(watchCtx, nodePath+"/", options...)
	if watcher == nil {
		return nil, nil, fmt.Errorf("Watch failed")
	}

	// Create the notifications channel, send updates to it.
	notifications := make(chan *topo.WatchData, 10)
	go func() {
		defer close(notifications)

		for {
			select {
			case <-watchCtx.Done():
				// This includes context cancelation errors.
				notifications <- &topo.WatchData{
					Err: convertError(watchCtx.Err()),
				}
				return
			case wresp := <-watcher:
				if wresp.Canceled {
					// Final notification.
					notifications <- &topo.WatchData{
						Err: convertError(wresp.Err()),
					}
					return
				}

				for _, ev := range wresp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						// send value deleted
						notifications <- &topo.WatchData{
							Contents: ev.Kv.Value,
							Version:  EtcdVersion(ev.Kv.ModRevision),
						}
					case mvccpb.DELETE:
						// return node path, that excluded root path
						keyDel := strings.Replace(string(ev.Kv.Key), c.root, "", 1)

						// send key deleted
						notifications <- &topo.WatchData{
							Contents: []byte(keyDel),
							Version:  EtcdVersion(ev.Kv.ModRevision),
							Err:      topo.ErrNoNode,
						}
					default:
						notifications <- &topo.WatchData{
							Err: fmt.Errorf("unexpected event received: %v", ev),
						}
						return
					}
				}
			}
		}
	}()

	return notifications, topo.CancelFunc(watchCancel), nil
}
