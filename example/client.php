<?php

$config = parse_ini_file("./config.ini", true);
$logger = new Logger($config['server']['ip'], (int)$config['server']['port']);

$startTime = microtime(true);

for ($i = 0; $i < 1000; $i++) {
	$logger->log('group1', 'некая utf8 строка');
}

for ($i = 0; $i < 10000; $i++) {
	$logger->incr('counter1');
}

printf("Time: %.5f s", microtime(true) - $startTime) . PHP_EOL;

class Logger {
	protected $ip;
	protected $port;

	public function __construct(string $ip, int $port) {
		$this->ip = $ip;
		$this->port = $port;
	}

	public function log(string $groupName, $value): void {
		$value = serialize($value);
		$this->send(
			chr(1) # type=0x1
			. pack('c', strlen($groupName)) . $groupName
			. pack('N', time())
			. pack('n', strlen($value)) . $value
		);
	}

	public function incr(string $counterName): void {
		$this->send(
			chr(2) # type=0x2
			. pack('c', strlen($counterName)) . $counterName
			. pack('N', (int)floor(time() / 60) * 60)
		);
	}

	public function decr(string $counterName): void {
		$this->send(
			chr(2) # type=0x3
			. pack('c', strlen($counterName)) . $counterName
			. pack('N', (int)floor(time() / 60) * 60)
		);
	}

	private function send(string $data): void {
		$socket = socket_create(AF_INET, SOCK_DGRAM, SOL_UDP);
		socket_sendto($socket, $data, strlen($data), 0, $this->ip, $this->port);
		socket_close($socket);
	}
}

