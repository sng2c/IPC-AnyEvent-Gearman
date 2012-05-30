use Test::More tests=>1;
use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init($ERROR);

use AnyEvent;
use EV;
use IPC::AnyEvent::Gearman;
my $gid = fork();
if( !$gid )
{
    DEBUG "######## start_gearmand ########";
    exec('gearmand -p 9999');
    die('cannot gearmand');
}
my $cv = AE::cv;
my @childs;

my $ppid = $$;

eval{
$recv = IPC::AnyEvent::Gearman->new(servers=>['localhost:9999']);
DEBUG "<<<<< start CHILD ".$recv->channel."\n";
$recv->on_receive(sub{ 
    DEBUG "<<<<< RECV $_[0]\n";
    return "OK";
});
$recv->listen();
};
if( $@){
    ERROR $@;
    kill 9,$gid;
    exit;
}
my $tt = AE::timer 3,0,sub{
    kill 9,$gid;
    sleep(3);
    $gid = fork();
    if( !$gid )
    {
        DEBUG "######## start_gearmand ########";
        exec('gearmand -p 9999');
        die('cannot gearmand');
    }

};
my $t = AE::timer 8,0,sub{
        DEBUG ">>>>> SEND to child \n";
        my $ch = IPC::AnyEvent::Gearman->new(servers=>['localhost:9999']);
        $ch->on_send(sub{
            my ($ch,$res) = @_;
            DEBUG "res : $res";
            is $res,'OK';
            $cv->send if( $res eq 'OK' );
        });
        $ch->send('kill');
};

my $t2 = AE::timer 13,0,sub{$cv->send;};

$cv->recv;
undef $t;
undef $t2;
kill 9,$gid;
DEBUG "DEAD $$\n";
done_testing();
