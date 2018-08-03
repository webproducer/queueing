<?php
namespace Queueing;

use Amp\Emitter;


interface SubscriberInterface
{

    public function subscribe(): Subscription;

}
