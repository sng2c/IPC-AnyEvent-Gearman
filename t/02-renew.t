#!/usr/bin/perl
use Test::More tests=>7;
use Log::Log4perl qw(:easy); 
Log::Log4perl->easy_init($ERROR);

use IPC::AnyEvent::Gearman;

use AnyEvent;

my $ig = IPC::AnyEvent::Gearman->new(servers=>['localhost:9999']);

is $ig->pid,$$;

$ig->listen();
my $worker = $ig->worker;
my $client = $ig->client;

$ig->pid($$);
my $client2 = $ig->client;
my $worker2 = $ig->worker;
isnt $worker, $worker2, 'renew worker by pid';
is $client, $client, 'renew not client by pid';

$worker = $ig->worker;
$client = $ig->client;
$ig->prefix("test_prefix");
$client2 = $ig->client;
$worker2 = $ig->worker;
isnt $worker, $worker2, 'renew worker by prefix';
is $client, $client, 'renew not client by prefix';

$worker = $ig->worker;
$client = $ig->client;
$ig->servers(['localhost:9999']);
$client2 = $ig->client;
$worker2 = $ig->worker;
isnt $worker, $worker2, 'renew worker by servers';
isnt $client, $client2, 'renew client bt servers';

done_testing();

