use Test::More tests=>10;
use Log::Log4perl qw(:easy); 
Log::Log4perl->easy_init($ERROR);

use AnyEvent;
use EV;
use IPC::AnyEvent::Gearman;

my $pid = fork();
if( !$pid )
{
    exec('gearmand -p 9999');
    die('cannot gearmand');
}

sleep(3);
@childs;
$recv;

my $cv = AE::cv;
my $ppid = $$;
foreach (1..10){
    print "#$_\n";
    $pid = fork();
    if( $pid ){
        push(@childs, IPC::AnyEvent::Gearman->new(servers=>['localhost:9999'],pid=>$pid) );
    }

    else{
        $recv = IPC::AnyEvent::Gearman->new(servers=>['localhost:9999']);
        print "<<<<< start CHILD ".$recv->channel."\n";
        $recv->on_receive(sub{ 
            print "<<<<< RECV $_[0]\n";
            $cv->send if( $_[0] eq 'kill' );
            return "OK";
        });
        $recv->listen();
        $cv->recv;
        print "DEAD CHILD $$\n";

        exit;
    }
}


$t = AE::timer 5,0,sub{
    print ">>>>> SEND killall\n";
    foreach my $ch (@childs){
        $ch->on_send(sub{
            my ($ch,$res) = @_;
            is $res,'OK';
        });
        $ch->send('kill');
    }
};
$t2 = AE::timer 8,0,sub{$cv->send;};

$cv->recv;
print "DEAD $$\n";
done_testing();

