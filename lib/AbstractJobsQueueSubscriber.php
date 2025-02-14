<?php

namespace Queueing;

use Amp\Promise;
use Amp\Emitter;
use Amp\Delayed;
use Amp\Deferred;
use Amp\Sync\LocalSemaphore;
use Amp\Sync\Lock;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use function Amp\call;

abstract class AbstractJobsQueueSubscriber implements SubscriberInterface
{
    /** @var JobsQueueInterface */
    private $queue;
    private $isStopped = false;
    /** @var Deferred */
    private $stoppedDeffer;
    /** @var Emitter */
    private $emitter;
    private $results = [];
    /** @var JobFactoryInterface */
    private $jobFactory;
    /** @var int|null JobInterface|Bulk wait timeout in milliseconds */
    protected $waitTime = null;
    /** @var WaitGroup */
    private $waitResults;
    /** @var LoggerInterface */
    protected $logger;
    private $processingJobs = [];
    /** @var LocalSemaphore */
    private $semaphore;
    /** @var Lock[] */
    private $semaphoreLocks = [];

    /**
     * AbstractJobsQueueSubscriber constructor.
     * @param JobsQueueInterface $queue
     * @param JobFactoryInterface|null $jobFactory
     * @param LoggerInterface|null $logger
     * @param int $maxJobs
     */
    public function __construct(
        JobsQueueInterface  $queue,
        JobFactoryInterface $jobFactory = null,
        LoggerInterface     $logger = null,
        int                 $maxJobs = 1
    ) {
        $this->queue = $queue;
        $this->jobFactory = $jobFactory ?: new BaseFactory();
        $this->waitResults = new WaitGroup();
        $this->logger = $logger ?? new NullLogger();
        $this->semaphore = new LocalSemaphore($maxJobs);
        $this->stoppedDeffer = new Deferred();
    }

    public function isStopped(): bool {
        return $this->isStopped;
    }

    /**
     * @return JobsQueueInterface
     */
    public function getQueue(): JobsQueueInterface {
        return $this->queue;
    }

    /**
     * @return JobFactoryInterface
     */
    public function getJobFactory(): JobFactoryInterface {
        return $this->jobFactory;
    }

    /**
     * @param PerformingResult $result
     * @return Promise
     */
    public function sendResult(PerformingResult $result): Promise {
        $this->results[] = [$def = new Deferred(), $result];
        $this->waitResults->done();
        return $def->promise();
    }

    /**
     * @param int $milliseconds JobInterface|Bulk wait timeout in milliseconds
     * @return AbstractJobsQueueSubscriber
     */
    public function setMaxWaitTime(int $milliseconds): self {
        $this->waitTime = $milliseconds;
        return $this;
    }

    /**
     * @inheritdoc
     */
    abstract public function subscribe(): Subscription;

    protected function makeSubscription(): Subscription {
        $this->isStopped = false;
        $this->emitter = new Emitter();
        return new Subscription($this->emitter->iterate(), function () use (&$isStopped) {
            $this->stoppedDeffer->resolve(true);
            $this->isStopped = true;
        });
    }

    protected function nextJob($timeout = null): Promise {
        //TODO: implement through foreign lib func?
        return call(function () use ($timeout) {
            $lock = yield Promise\first([
                $this->semaphore->acquire(),
                $this->stoppedDeffer->promise(),
            ]);
            if ($this->isStopped || !($lock instanceof Lock)) {
                return null;
            }

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
                if ($result instanceof JobsQueueException) {
                    $context = ['processing_jobs' => array_keys($this->processingJobs)];
                    $this->logger->warning($result->getMessage(), $context);
                    $context['exception'] = $result;
                    $this->logger->debug($result->getMessage(), $context);
                    $result = self::TIMED_OUT;
                } else {
                    $lock->release();
                    throw $result;
                }
            }

            if (is_array($result)) {
                $jobId = $result[0];
                $this->processingJobs[$jobId] = microtime(true);
                $this->logger->debug('Job reserved', ['job_id' => $jobId]);
                $this->semaphoreLocks[$jobId] = $lock;
            }
            if ($result === self::TIMED_OUT || $this->isStopped) {
                $lock->release();
            }

            return $result;
        });
    }

    /**
     * @param array $jobData
     * @return JobInterface
     * @throws JobCreatingException
     */
    protected function makeJob(array $jobData): JobInterface {
        [$id, $payload] = $jobData;
        //TODO: handle JobCreatingException?
        return $this->jobFactory->makeJob($id, $payload);
    }

    protected function emit($value): Promise {
        return call(function () use ($value) {
            $this->waitResults->inc();
            yield $this->emitter->emit($value);
        });
    }

    protected function complete(): Promise {
        return call(function () {
//            $this->stoppedDeffer->resolve(true);
            $this->emitter->complete();
            $this->waitResults->lock();
            yield $this->waitResults;
            yield $this->processResults();
            if ($this->queue instanceof ClosableInterface) {
                $this->queue->close();
            }
            foreach ($this->semaphoreLocks as $lock) {
                $lock->release();
            }
        });
    }

    protected function processResults(): Promise {
        return call(function () {
            if (empty($this->results) && !empty($this->semaphoreLocks)) {
                yield $this->waitResults->waitForSomeIsDone();
            }

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

    private function processResult(PerformingResult $result): Promise {
        return call(function () use ($result) {
            /** @var JobInterface $job */
            foreach ($result->getDoneJobs() as $job) {
                $id = $job->getId();
                yield $this->queue->delete($id);
                if (isset($this->semaphoreLocks[$id])) {
                    $this->semaphoreLocks[$id]->release();
                }
                unset($this->processingJobs[$id], $this->semaphoreLocks[$id]);
                $this->logger->debug('Job deleted', ['job_id' => $id]);
            }
            /** @var PerformingException $error */
            foreach ($result->getErrors() as $error) {
                $id = $error->getJob()->getId();
                if ($error->needsToBeRepeated()) {
                    yield $this->queue->release($id, $error->getRepeatDelay());
                    if (isset($this->semaphoreLocks[$id])) {
                        $this->semaphoreLocks[$id]->release();
                    }
                    unset($this->processingJobs[$id], $this->semaphoreLocks[$id]);
                    $this->logger->debug('Job released', ['job_id' => $id]);
                    continue;
                }
                yield $this->queue->bury($id);
                if (isset($this->semaphoreLocks[$id])) {
                    $this->semaphoreLocks[$id]->release();
                }
                unset($this->processingJobs[$id], $this->semaphoreLocks[$id]);
                $this->logger->debug('Job buried', ['job_id' => $id]);
            }
        });
    }
}
