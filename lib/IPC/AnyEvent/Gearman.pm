package IPC::AnyEvent::Gearman;
# ABSTRACT: IPC through gearmand.
use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init($DEBUG);
use Any::Moose;
use namespace::autoclean;

use Data::Dumper;
use AnyEvent::Gearman;
use Devel::GlobalDestruction;

=pod


=head1 SYNOPSIS

    use AnyEvent;
    use IPC::AnyEvent::Gearman;
    
    #receive    
    my $recv = IPC::AnyEvent::Gearman->new(servers=>['localhost:9999']);
    $recv->on_receive(sub{
        my $msg = shift;
        print "received msg : $data\n";
        return "OK";#result
    });
    $recv->listen();

    my $cv = AE::cv;
    $cv->recv;

    #send
    my $send = IPC::AnyEvent::Gearman->new(server=>['localhost:9999']);
    $send->pid(1102);
    my $result = $send->send("TEST DATA");
    
=cut

# VERSION

=head1 FUNCTIONS

=head3 new

=cut
=head4 pid
'pid' is unique id for identifying each process.
This can be any value not just PID.
It is filled own PID by default.
=cut

has 'pid' => (is => 'rw', isa => 'Str', default=>sub{return $$;});

=head4 servers
ArrayRef of hosts.
=cut
has 'servers' => (is => 'rw', isa => 'ArrayRef',required => 1);

=head4 prefix
When register function, it uses prefix+pid as function name.
It is filled 'IPC::AnyEvent::Gearman#' by default. 
=cut
has 'prefix' => (is => 'rw', isa => 'Str', default=>'IPC::AnyEvent::Gearman#');

=head4 on_receive
on_receive Hander.
First argument is DATA which is sent.
This can be invoked after listen().
=cut
has 'on_receive' => (is => 'rw', isa=>'CodeRef', 
    default=>sub{return sub{WARN 'You need to set on_receive function'};}
);
=head4 on_send
on_send handler.
First argument is a channel string.
=cut
has 'on_send' => (is => 'rw', isa=>'CodeRef', 
    default=>sub{return sub{INFO 'Send OK '.$_[0]};}
);
=head4 on_sendfail
on_sendfail handler.
First argument is a channel string.
=cut
has 'on_sendfail' => (is => 'rw', isa=>'CodeRef', 
    default=>sub{return sub{WARN 'Send FAIL '.$_[0]};}
);

has 'client' => (is=>'rw', lazy=>1, isa=>'Object',
default=>sub{
    DEBUG 'lazy client';
    my $self = shift;
    return gearman_client @{$self->servers()};
},
);

has 'worker' => (is=>'rw', isa=>'Object',
                    );

after 'pid' => sub{
    my $self = shift;
    if( @_ && $self->{listening}){
        $self->_renew_connection();    
    }
};

after 'prefix' => sub{
    my $self = shift;
    if( @_ && $self->{listening}){
        $self->_renew_connection();    
    }
};

after 'servers' => sub{
    my $self = shift;
    if( @_ && $self->{listening}){
        $self->_renew_connection();    
    }
    if( @_ ){
        $self->client( gearman_client @{$self->servers()} );
    }
};

=head4 listen
To receive message, you MUST call listen().
=cut
sub listen{
    my $self = shift;
    $self->{listening} = 1;
    $self->_renew_connection();
}
=head4 channel
get prefix+pid
=cut
sub channel{
    my $self = shift;
    return $self->prefix().$self->pid();
}
=head4 send
To send data to process listening prefix+pid, use this.
You must set 'pid' or 'prefix' attribute on new() method.

    my $send = IPC::AnyEvent::Gearman->new(pid=>1223);

=cut
sub send{
    my $self = shift;
    my $data = shift;
    $self->client->add_task(
        $self->channel() => $data,
        on_complete => sub{
            my $result = $_[1];
            $self->on_send()->($self->channel(),$_[1]);
        },
        on_fail => sub{
            $self->on_sendfail()->($self->channel());
        }
    );
}

sub _renew_connection{
    my $self = shift;
    DEBUG "new Connection";
    $self->worker(gearman_worker @{$self->servers()});
    $self->worker->register_function(
        $self->prefix().$self->pid() => sub{
            my $job = shift;
            my $res = $self->on_receive()->($job->workload);
            $res = '' unless defined($res);
            $job->complete($res);
        }
    );
    
}
sub BUILD{
    my $self = shift;
    DEBUG $self->channel." BUILD";
}
sub DEMOLISH{   
    return if in_global_destruction();
    my $self = shift;
    DEBUG $self->channel." DEMOLISH";
}
__PACKAGE__->meta->make_immutable(inline_constructor => 0);

1;
