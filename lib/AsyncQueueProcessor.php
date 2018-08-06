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
    )
    {
        $this->performer = $performer;
        $this->jobFactory = $jobFactory ?: new BaseFactory();
    }

    public function process(JobsQueueInterface $queue, int $bulkSize = 1): Promise
    {
        $subscriber = $this->makeSubscriber($queue, $bulkSize);
        $this->subscription = $subscriber->subscribe();
        return call(function() use ($subscriber) {
            while (yield $this->subscription->advance()) {
                yield $subscriber->sendResult(
                    yield $this->perform($this->subscription->getCurrent())
                );
            }
        });
    }

    public function stop() {
        $this->subscription->cancel();
    }

    private function makeSubscriber(JobsQueueInterface $queue, int $bulkSize): AbstractJobsQueueSubscriber
    {
        if ($bulkSize > 1) {
            return (new JobsQueueBulkSubscriber($queue, $this->jobFactory))->setBulkSize($bulkSize);
        }
        return new JobsQueueSubscriber($queue, $this->jobFactory);
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
        try {
            $result = $this->performer->perform($job);
            if (!($result instanceof Promise)) {
                $result = new Success($result);
            }
            return call(function () use ($job, $result) {
                return $this->wrapResult($job, yield $result);
            });
        } catch (PerformingException $e) {
            return new Failure($e);
        }
    }

    private function performBulk(Bulk $bulk): Promise
    {
        if ($this->performer instanceof BulkJobPerformerInterface) {
            $result = $this->performer->bulkPerform($bulk);
            return (!($result instanceof Promise)) ? new Success($result) : $result;
        }
        return call(function() use ($bulk) {
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
