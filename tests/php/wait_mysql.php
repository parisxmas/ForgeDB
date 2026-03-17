<?php
mysqli_report(MYSQLI_REPORT_OFF);
for ($i = 0; $i < 30; $i++) {
    $c = @new mysqli('127.0.0.1', 'root', '', '', 3306);
    if (!$c->connect_error) {
        echo "READY\n";
        $c->close();
        exit(0);
    }
    sleep(2);
    echo "waiting...\n";
}
echo "TIMEOUT\n";
exit(1);
