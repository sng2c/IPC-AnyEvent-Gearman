package IPC::AnyEvent::Gearman;
# ABSTRACT: turns baubles into trinkets
use strict;
use warnings;
use Any::Moose;
use Data::Dumper;
use AnyEvent::Gearman;
use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init($DEBUG);

# VERSION

has 'pid' => (is => 'rw', isa => 'Str', default=>sub{return $$;});
has 'servers' => (is => 'rw', isa => 'ArrayRef',required => 1);
has 'prefix' => (is => 'rw', isa => 'Str', default=>'IPC::AnyEvent::Gearman#');
has 'on_receive' => (is => 'rw', isa=>'CodeRef', 
    default=>sub{return sub{WARN 'You need to set on_receive function'};}
);
has 'on_send' => (is => 'rw', isa=>'CodeRef', 
    default=>sub{return sub{INFO 'Send OK '.$_[0]};}
);
has 'on_sendfail' => (is => 'rw', isa=>'CodeRef', 
    default=>sub{return sub{WARN 'Send FAIL '.$_[0]};}
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

sub BUILD{
    my $self = shift;
    $self->{client} = gearman_client @{$self->servers()};
};

sub listen{
    my $self = shift;
    $self->{listening} = 1;
    $self->_renew_connection();
}

sub channel{
    my $self = shift;
    return $self->prefix().$self->pid();
}

sub send{
    my $self = shift;
    my $data = shift;
    $self->{client}->add_task(
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
    if(defined($self->{'worker'})){
        undef($self->{'worker'});
        delete $self->{'worker'};
        DEBUG "renew Connection";
        return;
    }
    DEBUG "new Connection";
    $self->{worker} = gearman_worker @{$self->servers()};
    $self->{worker}->register_function(
        $self->prefix().$self->pid() => sub{
            my $job = shift;
            my $res = $self->on_receive()->($job->workload);
            $res = '' unless defined($res);
            $job->complete($res);
        }
    );
    
}

sub DESTROY{
    my $self = shift;
    DEBUG $self->channel()." Destroyed";
}

1;
