<?php
namespace Queueing;

use Amp\Delayed;
use Amp\Emitter;
use Amp\Promise;

use function Amp\call;
use function Amp\Promise\first;



class JobsQueueSubscriber implements SubscriberInterface
{
    /** @var JobsQueueInterface */
    private $queue;
    private $isStopped = false;

    /**
     * JobsQueueSubscriber constructor.
     * @param JobsQueueInterface $queue - queue implementation with async mode enabled
     */
    public function __construct(JobsQueueInterface $queue)
    {
        $this->queue = $queue;
    }

    public function subscribe(): Subscription
    {
        $this->isStopped = false;
        $emitter = new Emitter;
        $sub = new Subscription($emitter->iterate(), function() use (&$isStopped) {
            $this->isStopped = true;
        });
        $exited = $this->exited();
        call(function() use ($emitter, $exited) {
            //TODO: use timeout to cancel Promise returned by reserve() on exiting?
            while ($data = yield first([$this->queue->reserve(), $exited])) {
                //TODO: do we need wait for emitting here?
                yield $emitter->emit($data);
            }
            $emitter->complete();
        });
        return $sub;
    }

    private function exited(): Promise
    {
        return call(function () use (&$isStopped) {
            while (!$this->isStopped) {
                yield new Delayed(50);
            }
            return false;
        });
    }


}
