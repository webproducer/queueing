<?php
namespace Queueing;

use Amp\Emitter;

use function Amp\call;


class SubscriberBulkWrapper implements SubscriberInterface
{

    /** @var SubscriberInterface */
    private $subscriber;
    private $portion = 1;

    /**
     * SubscriberBulkWrapper constructor.
     * @param SubscriberInterface $subscriber
     * @param int $portion
     * @todo Add max wait time?
     */
    public function __construct(SubscriberInterface $subscriber, int $portion = 1)
    {
        $this->subscriber = $subscriber;
        $this->portion = $portion;
    }

    public function subscribe(): Subscription
    {
        $emitter = new Emitter;
        $subscription = $this->subscriber->subscribe();
        $sub = new Subscription($emitter->iterate(), function() use ($subscription) {
            $subscription->cancel();
        });
        call(function() use ($emitter, $subscription) {
            $jobs = [];
            while ($jobData = yield $subscription->advance()) {
                $jobs[] = $jobData;
                if (count($jobs) === $this->portion) {
                    //TODO: do we need wait for emitting here?
                    yield $emitter->emit($jobs);
                    $jobs = [];
                }
            }
            if (count($jobs) > 0) {
                //TODO: do we need wait for emitting here?
                yield $emitter->emit($jobs);
            }
            $emitter->complete();
        });
        return $sub;
    }


}
