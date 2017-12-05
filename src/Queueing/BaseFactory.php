<?php
namespace Queueing;

class BaseFactory implements IJobFactory
{

    private $class;

    /**
     * BaseFactory constructor.
     * @param string $jobClassname
     */
    public function __construct($jobClassname = BaseJob::class) {
        $this->class = $jobClassname;
    }

    /**
     * @inheritDoc
     */
    public function makeJob(int $id, string $payload): IJob {
        return call_user_func_array([$this->class, 'createWithIdAndPayload'], [$id, $payload]);
    }

}
