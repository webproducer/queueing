<?php
namespace Queueing;


class BaseJobFactory implements IJobFactory
{
    /**
     * @inheritDoc
     */
    public function makeJob(int $id, string $payload): IJob {
        return BaseJob::createWithIdAndPayload($id, $payload);
    }

}
