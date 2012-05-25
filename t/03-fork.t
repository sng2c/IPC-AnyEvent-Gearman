use Test::More tests=>10;
use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init($ERROR);

use AnyEvent;
use EV;
use IPC::AnyEvent::Gearman;

my $gid = fork();
if( !$gid )
{
    exec('gearmand -p 9999');
    die('cannot gearmand');
}

sleep(3);
my @childs;

my $cv = AE::cv;
my $ppid = $$;
foreach (1..10){
    DEBUG "#$_\n";
    $pid = fork();
    if( $pid ){
        push(@childs, $pid);
    }

    else{
        $recv = IPC::AnyEvent::Gearman->new(servers=>['localhost:9999']);
        DEBUG "<<<<< start CHILD ".$recv->channel."\n";
        $recv->on_receive(sub{ 
            DEBUG "<<<<< RECV $_[0]\n";
            $cv->send if( $_[0] eq 'kill' );
            return "OK";
        });
        $recv->listen();
        $cv->recv;
        undef $recv;
        DEBUG "DEAD CHILD $$\n";

        exit;
    }
}


my $t = AE::timer 5,0,sub{
    DEBUG ">>>>> SEND killall\n";
    foreach my $pid (@childs){
        my $ch = IPC::AnyEvent::Gearman->new(servers=>['localhost:9999'],pid=>$pid);
        $ch->on_send(sub{
            my ($ch,$res) = @_;
            is $res,'OK';
        });
        $ch->send('kill');
    }
};
my $t2 = AE::timer 8,0,sub{$cv->send;};

$cv->recv;
undef $t;
undef $t2;
DEBUG "DEAD $$\n";
kill 9,$gid;
done_testing();

