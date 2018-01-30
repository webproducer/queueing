<?php
namespace Queueing;


class BaseJobInterface implements JobInterface
{
    private $_id = 0;

    private $_data = null;

    /**
     * AbstractJob constructor.
     * @param string $data
     * @param int $id
     */
    protected function __construct(string $data = '', int $id = 0) {
        $this->_id = $id;
        $this->_data = $data;
    }

    /**
     * @param int $id
     * @param string $payload
     * @return static
     */
    public static function createWithIdAndPayload(int $id, string $payload) {
        return new static($payload, intval($id));
    }

    /**
     * @inheritdoc
     */
    public function getPayload(): string {
        return $this->_data;
    }

    /**
     * @inheritdoc
     */
    public function getId(): int {
        return $this->_id;
    }

    /**
     * @return array
     * @throws Exception
     */
    public function getJsonData(): array {
        $data = json_decode($this->_data, true);
        if (is_null($data)) {
            throw new Exception(sprintf("Can't parse JSON data: %s", json_last_error_msg()));
        }
        return $data;
    }


}
