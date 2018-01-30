<?php
namespace Queueing;


class JobPerformerBulkWrapper implements BulkJobPerformerInterface
{
    private $performer;

    public function __construct(JobPerformerInterface $performer)
    {
        $this->performer = $performer;
    }

    /**
     * @inheritDoc
     */
    public function bulkPerform(array $jobs): BulkPerformingResult
    {
        $result = new BulkPerformingResult();
        foreach ($jobs as $job) {
            try {
                $this->performer->perform($job);
                $result->setAsDone($job);
            } catch (PerformingException $e) {
                $result->addError($e->setJob($job));
            } catch (\Throwable $e) {
                $throw = new BulkPerformingException("Bulk partial performed: {$e->getMessage()}");
                throw $throw->setResult($result)->setSrcException($e);
            }
        }
        return $result;
    }


}
