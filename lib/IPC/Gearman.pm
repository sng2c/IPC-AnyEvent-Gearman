package IPC::Gearman;
# ABSTRACT: turns baubles into trinkets
use strict;
use warnings;
use Any::Moose;
use UUID::Random;
use Data::Dumper;
use AnyEvent::Gearman;
use Log::Log4perl qw(:easy);
Log::Log4perl->easy_init($DEBUG);

# VERSION

has 'channel' => (is => 'rw', isa => 'Str', default=>sub{return UUID::Random::generate;});
has 'servers' => (is => 'rw', isa => 'ArrayRef',required => 1);

after 'channel' => sub{
    my $self = shift;
    if( @_ ){
        $self->_renew_connection();    
    }
};

sub BUILD{
    my $self = shift;
    $self->_renew_connection();
};

sub _renew_connection{
    my $self = shift;
    if(defined($self->{'worker'})){
        undef($self->{'worker'});
        delete $self->{'worker'};
        DEBUG "renew Connection";
        return;
    }
    DEBUG "new Connection";
    $self->{'worker'} = gearman_worker @{$self->servers()};
}
1;
