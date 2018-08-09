<?php
namespace Queueing;

use Amp\Emitter;


interface SubscriberInterface
{

    const TIMED_OUT = 'TIMED_OUT';

    public function subscribe(): Subscription;

}
