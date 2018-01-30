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
        $this->_jobFactory = new BaseFactory();
    }

    public function setJobPerformer(JobPerformerInterface $performer): self
    {
        $this->_performer = $performer;
        return $this;
    }

    public function setJobFactory(JobFactoryInterface $f): self
    {
        $this->_jobFactory = $f;
        return $this;
    }

    public function bulkProcess(JobsQueueInterface $queue, int $bulkLimit = 10): \Generator
    {
        $performer = $this->_performer;
        if (!is_null($performer) && !($performer instanceof BulkJobPerformerInterface)) {
            $performer = new JobPerformerBulkWrapper($performer);
        }
        $jobs = [];
        while (true) {
            /** @var JobInterface $job */
            $job = null;
            list($id, $data) = $queue->reserve($this->_jobWaitTimeout);
            if ($id) {
                try {
                    $job = $this->_jobFactory->makeJob($id, $data);
                    $jobs[] = $job;
                }  catch (JobCreatingException $e) {
                    $queue->bury($id);
                }
                if (count($jobs) < $bulkLimit) {
                    continue;
                }
            }
            if (count($jobs) === 0) {
                yield null;
                continue;
            }
            $throwed = null;
            try {
                if ($performer) {
                    if ($error = yield $jobs) {
                        throw $error;
                    }
                    $result = $performer->bulkPerform($jobs);
                } else {
                    $result = yield $jobs;
                }
            } catch (BulkPerformingException $e) {
                $result = $e->getResult();
                $throwed = $e;
            }
            foreach ($result->getPerformed() as $job) {
                $queue->delete($job->getId());
            }
            foreach ($result->getErrors() as $e) {
                $this->processPerformingError($queue, $e);
            }
            if ($throwed) {
                throw $throwed;
            }
            $jobs = [];
        }
    }

    /**
     * @param JobsQueueInterface $queue
     * @return \Generator
     * @throws \Exception
     */
    public function process(JobsQueueInterface $queue): \Generator
    {
        while (true) {
            /** @var JobInterface $job */
            $job = null;
            $id = 0;
            try {
                list($id, $data) = $queue->reserve($this->_jobWaitTimeout);
                if (!$id) {
                    yield null;
                    continue;
                }
                $job = $this->_jobFactory->makeJob($id, $data);
                if ($error = yield $job) {
                    throw $error;
                }
                if ($this->_performer) {
                    $this->_performer->perform($job);
                }
                $queue->delete($id);
            } catch (JobCreatingException $e) {
                $queue->bury($id);
            } catch (PerformingException $e) {
                $this->processPerformingError($queue, $e->setJob($job));
            }
        }
    }

    private function processPerformingError(JobsQueueInterface $q, PerformingException $e) {
        $job = $e->getJob();
        if ($e->needsToBeRepeated()) {
            $q->release($job->getId(), $e->getRepeatDelay());
            return;
        }
        $q->bury($job->getId());
    }

}
