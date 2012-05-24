#!/usr/bin/perl
use Test::More;

use IPC::AnyEvent::Gearman;

use AnyEvent;

my $pid = fork();
if( !$pid )
{
    exec('gearmand -p 9999');
    die('cannot gearmand');
}
sleep(3);
my $cv = AE::cv;
my $ig = IPC::AnyEvent::Gearman->new(servers=>['localhost:9999']);

is $ig->pid,$$;
$ig->on_receive(sub{
    my $data = shift;
    is $data, 'TEST';
    $cv->send;
});

$ig->listen();

my $ig2 = IPC::AnyEvent::Gearman->new(servers=>['localhost:9999']);
$ig2->send('TEST');

$cv->recv;
kill 9,$pid;

done_testing();
