<?php

namespace Queueing;

use Amp\{Promise, Emitter, Delayed, Deferred};
use function Amp\call;

abstract class AbstractJobsQueueSubscriber implements SubscriberInterface
{
    /** @var JobsQueueInterface */
    private $queue;
    private $isStopped = false;
    /** @var Emitter */
    private $emitter;
    private $results = [];
    /** @var JobFactoryInterface */
    private $jobFactory;
    /** @var int|null JobInterface|Bulk wait timeout in milliseconds */
    protected $waitTime = null;
    private $waitResults;

    /**
     * AbstractJobsQueueSubscriber constructor.
     * @param JobsQueueInterface $queue
     * @param JobFactoryInterface|null $jobFactory
     */
    public function __construct(JobsQueueInterface $queue, JobFactoryInterface $jobFactory = null)
    {
        $this->queue = $queue;
        $this->jobFactory = $jobFactory ?: new BaseFactory();
        $this->waitResults = new WaitGroup();
    }

    /**
     * @return JobsQueueInterface
     */
    public function getQueue(): JobsQueueInterface
    {
        return $this->queue;
    }

    /**
     * @return JobFactoryInterface
     */
    public function getJobFactory(): JobFactoryInterface
    {
        return $this->jobFactory;
    }

    public function sendResult(PerformingResult $result): Promise
    {
        $this->results[] = [$def = new Deferred(), $result];
//        $resultDef = array_shift($this->performingResultsPromises);
//        if ($resultDef) {
//            $resultDef->resolve();
//        }
        $this->waitResults->done();
        return $def->promise();
    }

    /**
     * @param int $milliseconds JobInterface|Bulk wait timeout in milliseconds
     * @return AbstractJobsQueueSubscriber
     */
    public function setMaxWaitTime(int $milliseconds): self
    {
        $this->waitTime = $milliseconds;
        return $this;
    }

    /**
     * @inheritdoc
     */
    abstract public function subscribe(): Subscription;

    protected function makeSubscription(): Subscription
    {
        $this->isStopped = false;
        $this->emitter = new Emitter();
        return new Subscription($this->emitter->iterate(), function () use (&$isStopped) {
            $this->isStopped = true;
        });
    }

    protected function nextJob($timeout = null): Promise
    {
        //TODO: implement through foreign lib func?
        return call(function () use ($timeout) {
            $result = null;
            $this->getQueue()->reserve($timeout)->onResolve(function ($e, $value) use (&$result) {
                $result = $e ?: (is_null($value) ? self::TIMED_OUT : $value);
            });
            $delay = 5;
            $maxDelay = 100;
            while (!$this->isStopped && is_null($result)) {
                //TODO: can we do this more effective way?
                yield new Delayed(($delay === $maxDelay) ? $maxDelay : $delay++);
            }
            if ($result instanceof \Throwable) {
                throw $result;
            }
            return $result;
        });
    }

    protected function makeJob(array $jobData): JobInterface
    {
        [$id, $payload] = $jobData;
        //TODO: handle JobCreatingException?
        return $this->jobFactory->makeJob($id, $payload);
    }

    protected function emit($value): Promise
    {
        return call(function () use ($value) {
            $this->waitResults->inc();
            yield $this->emitter->emit($value);
        });
    }

    protected function complete(): Promise
    {
        return call(function () {
            $this->emitter->complete();
            $this->waitResults->lock();
            yield $this->waitResults;
            yield $this->processResults();
        });
    }

    protected function processResults(): Promise
    {
        return call(function () {
            /**
             * @var Deferred $def
             * @var PerformingResult $result
             */
            foreach ($this->results as [$def, $result]) {
                yield $this->processResult($result);
                $def->resolve();
            }
            $this->results = [];
        });
    }

    private function processResult(PerformingResult $result): Promise
    {
        return call(function () use ($result) {
            /** @var JobInterface $job */
            foreach ($result->getDoneJobs() as $job) {
                yield $this->queue->delete($job->getId());
            }
            /** @var PerformingException $error */
            foreach ($result->getErrors() as $error) {
                $id = $error->getJob()->getId();
                if ($error->needsToBeRepeated()) {
                    yield $this->queue->release($id, $error->getRepeatDelay());
                    continue;
                }
                yield $this->queue->bury($id);
            }
        });
    }
}
