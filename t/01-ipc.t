#!/usr/bin/perl
use Test::More;

use IPC::Gearman;

use AnyEvent;

my $pid = fork();
if( !$pid )
{
    exec('gearmand -p 9999');
    exit;
}

my $cv = AE::cv;
my $ig = IPC::Gearman->new(servers=>['localhost:9999']);

$ig->channel('123');
is $ig->channel,'123';



$cv->recv;
kill 9,$pid;
