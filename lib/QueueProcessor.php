<?php
namespace Queueing;

class QueueProcessor
{
    const DEFAULT_WAIT_TIMEOUT = 2;    // 2 seconds

    private $_jobWaitTimeout = self::DEFAULT_WAIT_TIMEOUT;

    /** @var JobPerformerInterface */
    private $_performer;

    /** @var JobFactoryInterface */
    private $_jobFactory;

    public function __construct(
        $jobWaitTimeout = self::DEFAULT_WAIT_TIMEOUT
    ) {
        $this->_jobWaitTimeout = $jobWaitTimeout;
        $this->_jobFactory = new BaseFactoryInterface();
    }

    public function setJobPerformer(JobPerformerInterface $performer) {
        $this->_performer = $performer;
        return $this;
    }

    public function setJobFactory(JobFactoryInterface $f) {
        $this->_jobFactory = $f;
        return $this;
    }

    /**
     * @param JobsQueueInterface $queue
     * @return \Generator
     * @throws \Exception
     */
    public function process(JobsQueueInterface $queue) {
        while (true) {
            /** @var JobInterface $job */
            $job = null;
            try {
                list($id, $data) = $queue->reserve($this->_jobWaitTimeout);
                if (!$id) {
                    yield null;
                    continue;
                }
                $job = $this->_jobFactory->makeJob($id, $data);
                if (!($job instanceof JobInterface)) {
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
