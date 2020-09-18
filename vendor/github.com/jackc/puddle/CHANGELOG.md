# 1.1.1 (April 2, 2020)

* Pool.Close can be safely called multiple times
* AcquireAllIDle immediately returns nil if pool is closed
* CreateResource checks if pool is closed before taking any action
* Fix potential race condition when CreateResource and Close are called concurrently. CreateResource now checks if pool is closed before adding newly created resource to pool.

# 1.1.0 (February 5, 2020)

* Use runtime.nanotime for faster tracking of acquire time and last usage time.
* Track resource idle time to enable client health check logic. (Patrick Ellul)
* Add CreateResource to construct a new resource without acquiring it. (Patrick Ellul)
* Fix deadlock race when acquire is cancelled. (Michael Tharp)
