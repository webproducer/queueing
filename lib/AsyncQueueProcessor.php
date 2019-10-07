<?php
namespace Queueing;

use Amp\Promise;
use Amp\Success;
use function Amp\call;

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
        return (new JobsQueueSubscriber($queue, $this->jobFactory))->setMaxWaitTime(intval($maxWaitTime));
    }

    /**
     * @param JobInterface|Bulk $jobData
     * @return Promise
     */
    private function perform($jobData): Promise
    {
        switch (true) {
            case $jobData instanceof JobInterface:
                return call(function () use ($jobData) {
                    try {
                        return $this->wrapResult($jobData, yield $this->performSingle($jobData));
                    } catch (PerformingException $e) {
                        return PerformingResult::fail($e->setJob($jobData));
                    }
                });
            case $jobData instanceof Bulk:
                return $this->performBulk($jobData);
            default:
                throw new \InvalidArgumentException("Argument must be an instance of JobInterface|Bulk");
        }
    }

    /**
     * @param JobInterface $job
     * @return Promise
     * @throws PerformingException
     */
    private function performSingle(JobInterface $job): Promise
    {
        $result = $this->performer->perform($job);
        return ($result instanceof Promise) ? $result : new Success($result);
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
