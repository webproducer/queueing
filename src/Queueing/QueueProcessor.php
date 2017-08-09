<?php
namespace Queueing;

class QueueProcessor
{
    const DEFAULT_WAIT_TIMEOUT = 5;    // 5 seconds
    const DEFAULT_SLEEP_INTERVAL = 10; // 10 seconds

    private $_jobWaitTimeout = self::DEFAULT_WAIT_TIMEOUT;
    private $_sleepOnNoJob = self::DEFAULT_SLEEP_INTERVAL;
    private $_errorHandler = null;

    public function __construct(
        $jobWaitTimeout = self::DEFAULT_WAIT_TIMEOUT,
        $sleepOnNoJob = self::DEFAULT_SLEEP_INTERVAL
    ) {
        $this->_jobWaitTimeout = $jobWaitTimeout;
        $this->_sleepOnNoJob = $sleepOnNoJob;
    }

    /**
     * @param \Closure $callback
     * @return self
     */
    public function setErrorHandler(\Closure $callback) {
        $this->_errorHandler = $callback;
        return $this;
    }

    /**
     * @param IJobsQueue $queue
     * @return \Generator
     * @throws \Exception
     */
    public function process(IJobsQueue $queue) {
        while (true) {
            $job = null;
            try {
                $job = $queue->reserve($this->_jobWaitTimeout);
                if (is_null($job)) {
                    yield null;
                    sleep($this->_sleepOnNoJob);
                    continue;
                }
                $job->perform();
                $queue->delete($job);
                yield $job;
            } catch (\Exception $e) {
                $rethrow = true;
                if (!is_null($job)) {
                    $queue->bury($job);
                }
                if (!is_null($this->_errorHandler)) {
                    $rethrow = call_user_func_array($this->_errorHandler, [$e, $job]);
                }
                if ($rethrow) {
                    throw $e;
                }
            }
        }
    }

}
