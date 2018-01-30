<?php
namespace Queueing;

class BaseFactoryInterface implements JobFactoryInterface
{

    private $class;

    /**
     * BaseFactory constructor.
     * @param string $jobClassname
     */
    public function __construct($jobClassname = BaseJobInterface::class) {
        $this->class = $jobClassname;
    }

    /**
     * @inheritDoc
     */
    public function makeJob(int $id, string $payload): JobInterface {
        return call_user_func_array([$this->class, 'createWithIdAndPayload'], [$id, $payload]);
    }

}
