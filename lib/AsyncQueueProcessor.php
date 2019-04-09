<?php

namespace Queueing;

use Amp\Failure;
use Amp\Promise;

use function Amp\call;
use Amp\Success;

class AsyncQueueProcessor
{

    /** @var JobPerformerInterface */
    private $performer;

    /** @var JobFactoryInterface */
    private $jobFactory;

    /** @var Subscription */
    private $subscription;

    /**
     * AsyncQueueProcessor constructor.
     * @param JobPerformerInterface $performer
     * @param JobFactoryInterface|null $jobFactory
     */
    public function __construct(
        JobPerformerInterface $performer,
        JobFactoryInterface $jobFactory = null
    ) {
        $this->performer = $performer;
        $this->jobFactory = $jobFactory ?: new BaseFactory();
    }

    /**
     * @param JobsQueueInterface $queue
     * @param int $bulkSize - Max count of jobs in bulk
     * @param int|null $maxWaitTime - Max bulk|job awaiting time (in milliseconds)
     * @return Promise
     */
    public function process(
        JobsQueueInterface $queue,
        int $bulkSize = 1,
        int $maxWaitTime = null
    ): Promise {
        $subscriber = $this->makeSubscriber($queue, $bulkSize, $maxWaitTime);
        $this->subscription = $subscriber->subscribe();
        return call(function () use ($subscriber) {
            while (yield $this->subscription->advance()) {
                $subscriber->sendResult(
                    yield $this->perform($this->subscription->getCurrent())
                );
            }
        });
    }

    public function stop()
    {
        $this->subscription->cancel();
    }

    private function makeSubscriber(
        JobsQueueInterface $queue,
        int $bulkSize,
        int $maxWaitTime = null
    ): AbstractJobsQueueSubscriber {
        if ($bulkSize > 1) {
            return (new JobsQueueBulkSubscriber($queue, $this->jobFactory))
                ->setBulkSize($bulkSize)
                ->setMaxWaitTime(intval($maxWaitTime));
        }
        return (new JobsQueueSubscriber($queue, $this->jobFactory))->setMaxWaitTime($maxWaitTime);
    }

    /**
     * @param JobInterface|Bulk $jobData
     * @return Promise
     */
    private function perform($jobData): Promise
    {
        switch (true) {
            case $jobData instanceof JobInterface:
                return $this->performSingle($jobData);
            case $jobData instanceof Bulk:
                return $this->performBulk($jobData);
            default:
                throw new \InvalidArgumentException("Argument must be an instance of JobInterface|Bulk");
        }
    }

    private function performSingle(JobInterface $job): Promise
    {
        try { // TODO: Refactoring
            $result = $this->performer->perform($job); // Why it needs to be called outside of coroutine?
            if (!($result instanceof Promise)) {
                $result = new Success($result);
            }
            return call(function () use ($job, $result) {
                try {
                    return $this->wrapResult($job, yield $result);
                } catch (PerformingException $e) {
                    $e->setJob($job);
                    return (new PerformingResult)->registerError($e);
                }
            });
        } catch (PerformingException $e) {
            return new Failure($e); // TODO: Returning type should be a PerformingResult
        }
    }

    private function performBulk(Bulk $bulk): Promise
    {
        if ($this->performer instanceof BulkJobPerformerInterface) {
            $result = $this->performer->bulkPerform($bulk);
            return (!($result instanceof Promise)) ? new Success($result) : $result;
        }
        return call(function () use ($bulk) {
            $bulkResult = new PerformingResult();
            foreach ($bulk as $job) {
                try {
                    yield $this->performSingle($job);
                    $bulkResult->registerDoneJob($job);
                } catch (PerformingException $e) {
                    $bulkResult->registerError($e);
                }
            }
            return $bulkResult;
        });
    }

    private function wrapResult(JobInterface $job, $result): PerformingResult
    {
        return ($result instanceof PerformingResult) ? $result : PerformingResult::success($job);
    }
}
