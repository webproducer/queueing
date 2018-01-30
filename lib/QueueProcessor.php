<?php
namespace Queueing;

class QueueProcessor
{
    const DEFAULT_WAIT_TIMEOUT = 2;    // 2 seconds

    private $_jobWaitTimeout = self::DEFAULT_WAIT_TIMEOUT;

    /** @var IJobPerformer */
    private $_performer;

    /** @var IJobFactory */
    private $_jobFactory;

    public function __construct(
        $jobWaitTimeout = self::DEFAULT_WAIT_TIMEOUT
    ) {
        $this->_jobWaitTimeout = $jobWaitTimeout;
        $this->_jobFactory = new BaseFactory();
    }

    public function setJobPerformer(IJobPerformer $performer) {
        $this->_performer = $performer;
        return $this;
    }

    public function setJobFactory(IJobFactory $f) {
        $this->_jobFactory = $f;
        return $this;
    }

    /**
     * @param IJobsQueue $queue
     * @return \Generator
     * @throws \Exception
     */
    public function process(IJobsQueue $queue) {
        while (true) {
            /** @var IJob $job */
            $job = null;
            try {
                list($id, $data) = $queue->reserve($this->_jobWaitTimeout);
                if (!$id) {
                    yield null;
                    continue;
                }
                $job = $this->_jobFactory->makeJob($id, $data);
                if (!($job instanceof IJob)) {
                    throw new JobCreatingException("Job instance must implement IJob");
                }
                if ($this->_performer) {
                    $this->_performer->perform($job);
                }
                if ($error = yield $job) {
                    throw $error;
                }
                $queue->delete($id);
            } catch (JobCreatingException $e) {
                if (!is_null($job)) {
                    $queue->bury($job->getId());
                }
            } catch (PerformingException $e) {
                if (!is_null($job)) {
                    if ($e->needsToBeRepeated()) {
                        $queue->release($job->getId(), $e->getRepeatDelay());
                        continue;
                    }
                    $queue->bury($job->getId());
                }
            }
        }
    }

}
