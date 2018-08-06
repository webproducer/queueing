<?php
namespace Queueing;

use Amp\{ Promise, Emitter, Delayed, Deferred };

use function Amp\call;
use function Amp\Promise\first;


abstract class AbstractJobsQueueSubscriber implements SubscriberInterface
{
    /** @var JobsQueueInterface */
    private $queue;
    private $isStopped = false;
    /** @var Emitter */
    private $emitter;
    private $results = [];
    private $exitPromise;
    /** @var JobFactoryInterface */
    private $jobFactory;

    /**
     * AbstractJobsQueueSubscriber constructor.
     * @param JobsQueueInterface $queue
     * @param JobFactoryInterface|null $jobFactory
     */
    public function __construct(JobsQueueInterface $queue, JobFactoryInterface $jobFactory = null)
    {
        $this->queue = $queue;
        $this->jobFactory = $jobFactory ?: new BaseFactory();
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
        $this->results = [$def = new Deferred(), $result];
        return $def->promise();
    }

    /**
     * @inheritdoc
     */
    abstract public function subscribe(): Subscription;

    protected function makeSubscription(): Subscription
    {
        $this->isStopped = false;
        $this->emitter = new Emitter();
        return new Subscription($this->emitter->iterate(), function() use (&$isStopped) {
            $this->isStopped = true;
        });
    }

    protected function nextJob($timeout = null): Promise
    {
        return first([$this->getQueue()->reserve($timeout), $this->exited()]);
    }

    protected function makeJob(array $jobData): JobInterface
    {
        [$id, $payload] = $jobData;
        //TODO: handle JobCreatingException?
        return $this->jobFactory->makeJob($id, $payload);
    }

    protected function emit($value): Promise
    {
        return call(function() use ($value) {
            yield $this->emitter->emit($value);
            yield $this->processResults();
        });
    }

    protected function complete()
    {
        $this->emitter->complete();
        $this->exitPromise = null;
    }

    protected function exited(): Promise
    {
        if (!$this->exitPromise) {
            $this->exitPromise = call(function () use (&$isStopped) {
                while (!$this->isStopped) {
                    yield new Delayed(50);
                }
                return false;
            });
        }
        return $this->exitPromise;
    }

    private function processResults(): Promise
    {
        return call(function () {
            foreach ($this->results as $result) {
                yield $this->processResult($result);
            }
            $this->results = [];
        });
    }

    private function processResult(PerformingResult $result): Promise
    {
        return call(function() use ($result) {
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
