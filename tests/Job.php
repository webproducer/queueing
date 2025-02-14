<?php

namespace Queueing\Tests;

use Queueing\BaseJob;

class Job extends BaseJob
{
    public const DEFAULT_DURATION_MS = 50;

    private $data = [];

    /**
     * @param int $id
     * @param int $duration Duration of payload processing that will be emulated via 'Delayed'.
     * @return Job
     */
    public static function create(int $id, int $duration = self::DEFAULT_DURATION_MS): Job {
        return new self(json_encode([
            'id' => $id,
            'duration' => $duration,
        ]));
    }

    public function getPayloadId(): int {
        return $this->getData()['id'];
    }

    public function getDuration(): int {
        return $this->getData()['duration'];
    }

    private function getData(): array {
        if (empty($this->data)) {
            $payload = $this->getPayload();
            $data = json_decode($payload, true);
            if (json_last_error() !== JSON_ERROR_NONE) {
                throw new \RuntimeException(sprintf('JSON decode error: %s', json_last_error_msg()));
            }

            if (!is_array($data) || !isset($data['id']) || !isset($data['duration'])) {
                throw new \RuntimeException(sprintf('Invalid JSON payload: %s', $payload));
            }

            $this->data = $data;
        }

        return $this->data;
    }
}