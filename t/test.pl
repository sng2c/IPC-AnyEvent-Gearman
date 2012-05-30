package MyConnection;

use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init($DEBUG);

use namespace::autoclean;
use Scalar::Util 'weaken';
 
use AnyEvent;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Any::Moose;

use Data::Dumper;

has retry_count=>(is=>'rw',isa=>'Int',clearer=>'reset_retry',default=>0);
has retry_timer=>(is=>'rw',isa=>'Object',clearer=>'reset_timer');
extends 'AnyEvent::Gearman::Worker::Connection';
override connect=>sub{
    my ($self) = @_;
 
    # already connected
    return if $self->handler;
 
    my $g = tcp_connect $self->_host, $self->_port, sub {
        my ($fh) = @_;
 
        if ($fh) {
            my $handle = AnyEvent::Handle->new(
                fh       => $fh,
                on_read  => sub { $self->process_packet },
                on_error => sub {
                    my @undone = @{ $self->_need_handle },
                                 values %{ $self->_job_handles };
                    $_->event('on_fail') for @undone;
 
                    $self->_need_handle([]);
                    $self->_job_handles({});
                    $self->mark_dead;
                },
            );
            
            $self->reset_retry;
            $self->reset_timer;

            $self->handler( $handle );
            $_->() for map { $_->[0] } @{ $self->on_connect_callbacks };
        }
        else {
=pod
            if( $self->retry_count >= 3 )
            {
                warn sprintf("Connection failed: %s", $!);
                $self->mark_dead;
                $self->reset_retry;
                $self->reset_timer;
                $_->() for map { $_->[1] } @{ $self->on_connect_callbacks };
            }
            else
=cut
            {
                if( !$self->retry_timer ){
                    my $timer = AE::timer 0.1,0,sub{
                        DEBUG "retry connect";
                        $self->retry_count($self->retry_count+1);
                        $self->connect();
                        $self->reset_timer;
                    };
                    $self->retry_timer($timer);
                }
                return;
            }
        }
 
        $self->on_connect_callbacks( [] );
    };
 
    weaken $self;
    $self->_con_guard($g);
 
    $self;
};


package main;



use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init($DEBUG);

use AnyEvent;
use EV;
use AnyEvent::Gearman;



my $gid = fork();
if( !$gid )
{
    sleep(10);
    DEBUG "######## start_gearmand ########";
    exec('gearmand -p 9999');
    die('cannot gearmand');
}




my $cv = AE::cv;

my $ppid = $$;

use Data::Dumper;



    my $work = gearman_worker 'localhost:9999';
    my $js = $work->job_servers();
    DEBUG Dumper $js;
    $js->[0] = MyConnection->new(hostspec=>$js->[0]->hostspec);

    DEBUG Dumper $work->job_servers();


    $work->register_function(
    'reverse' => sub {
        my $job = shift;
        my $res = reverse $job->workload;
        DEBUG 'recv : '.$job->workload;
        $job->complete($res);
    },
    );






if( $@){
    ERROR $@;
    kill 9,$gid;
    exit;
}
my $t = AE::timer 13,0,sub{
        DEBUG ">>>>> SEND to child \n";
my $client = gearman_client 'localhost:9999';
 
$client->add_task(
    'reverse' => 'ABCDE',
    on_complete => sub {
        my $result = $_[1];
        DEBUG $result;
        # ...
    },
    on_fail => sub {
        # job failed
        DEBUG 'faile';
    },
);
};


my $t2 = AE::timer 15,0,sub{$cv->send;};

$cv->recv;
undef $t;
undef $t2;
DEBUG "DEAD $$\n";
kill 9,$gid;