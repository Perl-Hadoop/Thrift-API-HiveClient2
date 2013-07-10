package Thrift::API::HiveClient2;
# ABSTRACT: Perl to HiveServer2 Thrift API wrapper

=head1 WARNING

Thrift in Perl currently doesn't support SASL, so authentication needs
to be disabled for now on HiveServer2 by setting this property in your
/etc/hive/conf/hive-site.xml. Although the property is documented, this
*value* -which disables the SASL server transport- is not, AFAICT.

  <property>
    <name>hive.server2.authentication</name>
    <value>NOSASL</value>
  </property>

=cut

use strict;
use warnings;

use Moo;
use Carp;

use Thrift;
use Thrift::Socket;
use Thrift::BufferedTransport;

# Protocol loading is done dynamically later.

use Thrift::API::HiveClient2::TCLIService;

# Don't use XS for now, fails initializing properly with BufferedTransport. See
# Thrift::XS documentation.
has use_xs => (
    is      => 'rwp',
    default => sub {0},
    lazy    => 1,
);

has host => (
    is      => 'ro',
    default => sub {'localhost'},
);
has port => (
    is      => 'ro',
    default => sub {10_000},
);

# These exist to make testing with various other Thrift Implementation classes
# easier, eventually.

has _socket    => ( is => 'rwp' );
has _transport => ( is => 'rwp' );
has _protocol  => ( is => 'rwp' );
has _client    => ( is => 'rwp' );

# setters implied by the 'rwp' mode on the attrs above.

sub _set_socket    { $_[0]->{_socket}    = $_[1] }
sub _set_transport { $_[0]->{_transport} = $_[1] }
sub _set_protocol  { $_[0]->{_protocol}  = $_[1] }
sub _set_client    { $_[0]->{_client}    = $_[1] }

# after constructon is complete, initialize any attributes that
# weren't set in the constructor.
sub BUILD {
    my $self = shift;

    $self->_set_socket( Thrift::Socket->new( $self->host, $self->port ) )
        unless $self->_socket;

    $self->_set_transport( Thrift::BufferedTransport->new( $self->_socket ) )
        unless $self->_transport;

    $self->_set_protocol( $self->_init_protocol( $self->_transport ) )
        unless $self->_protocol;

    $self->_set_client( Thrift::API::HiveClient2::TCLIServiceClient->new( $self->_protocol ) )
        unless $self->_client;
}

=method new

Initialize the client object with the Hive server parameters

    my $client = Thrift::API::HiveClient2->new(
        host => <host name or IP, defaults to localhost>,
        port => <port, defaults to 10000>,
    );

=cut

sub _init_protocol {
    my $self = shift;
    my $err;
    my $protocol = eval {
        $self->use_xs
            && require Thrift::XS::BinaryProtocol;
        Thrift::XS::BinaryProtocol->new( $self->_transport );
    } or do { $err = $@; 0 };
    $protocol
        ||= do { require Thrift::BinaryProtocol; Thrift::BinaryProtocol->new( $self->_transport ) };
    $self->_set_use_xs(0) if ref($protocol) !~ /XS/;

    # TODO Add warning when XS was asked but failed to load
    return $protocol;
}

=method connect

Open the connection on the server declared in the object's constructor.

     $client->connect() or die "Failed to connect";

=cut

sub connect {
    my ($self) = @_;
    $self->_transport->open;
}

has _session => (
    is      => 'rwp',
    isa     => sub { 'Thrift::API::HiveClient2::TOpenSessionResp' },
    lazy    => 1,
    builder => '_build_session',
);

has username => (
    is      => 'rwp',
    isa     => sub {'Str'},
    lazy    => 1,
    default => sub {'foo'},
);

has password => (
    is      => 'rwp',
    isa     => sub {'Str'},
    lazy    => 1,
    default => sub {''},
);

sub _build_session {
    my $self = shift;
    $self->_transport->open if !$self->_transport->isOpen;
    return $self->_client->OpenSession(
        Thrift::API::HiveClient2::TOpenSessionReq->new(
            {   username => $self->username,
                password => $self->password,
            }
        )
    );
}

has _session_handle => (
    is      => 'rwp',
    isa     => sub {'Thrift::API::HiveClient2::TSessionHandle'},
    lazy    => 1,
    builder => '_build_session_handle',
);

sub _build_session_handle {
    my $self = shift;
    return $self->_session->{sessionHandle};
}

=method execute

Run an HiveQl statement on an open connection.

    my $rh = $client->execute( <HiveQL statement> );

=method fetch

Returns an array(ref) of arrayrefs, like DBI's fetchall_arrayref, and a boolean
indicator telling wether or not a subsequent call to fetch() will return more
rows.

    my ($rv, $has_more_rows) = $client->fetch( $rh, <maximum records to retrieve> );

=cut

sub execute {
    my $self = shift;
    my ($query) = @_;    # make this a bit more flexible
    my $rh = $self->_client->ExecuteStatement(
        Thrift::API::HiveClient2::TExecuteStatementReq->new(
            { sessionHandle => $self->_session_handle, statement => $query }
        )
    );
    if ($rh->{status}{errorCode}) {
        die "execute() failed: $rh->{errorMessage} (code: $rh->{errorCode})";
    }
    return $rh;
}

{
    # cache the column names we need to extract from the bloated data structure
    # (keyed on query)
    my $column_keys;

    sub fetch {
        my $self = shift;
        my ( $rv, $rows_at_a_time ) = @_;
        my $result = [];
        my $has_more_rows;
        my $rh = $self->_client->FetchResults(
            Thrift::API::HiveClient2::TFetchResultsReq->new(
                {   operationHandle => $rv->{operationHandle},
                    maxRows         => $rows_at_a_time || 10_000
                }
            )
        );
        if ( ref $rh eq 'Thrift::API::HiveClient2::TFetchResultsResp' ) {
            $has_more_rows = $rh->{hasMoreRows};

            for my $row ( @{ $rh->{results}{rows} || [] } ) {

                # Find which fields to extract from each row, only on the first iteration
                if ( !@{ $column_keys->{ $rv } || [] } ) {
                    for my $column ( @{ $row->{colVals} || [] } ) {

                        my $first_col = {%$column};

                        # Only 1 element of each TColumnValue is populated
                        # (although 7 keys are present, 1 for each possible data
                        # type) with a T*Value, and the rest is undef. Find out
                        # which is defined, and put the key (i.e. the data type) in
                        # cache, to reuse it to fetch the next rows faster.
                        # NOTE this data structure smells of Java and friends from
                        # miles away. Dynamically typed languages don't really need
                        # the bloat.
                        push @{ $column_keys->{$rv} }, grep { ref $first_col->{$_} } keys %$first_col;
                    }
                }

                # TODO find something faster?

                my $idx = 0;
                push @$result,
                    [ map { $row->{colVals}[ $idx++ ]{$_}->value() } @{ $column_keys->{$rv} } ];
            }
        }
        return $result, $has_more_rows;
    }
}

sub DEMOLISH {
    my $self = shift;
    return if !$self->_session_handle;
    $self->_client->CloseSession(
        Thrift::API::HiveClient2::TCloseSessionReq->new(
            { sessionHandle => $self->_session_handle, }
        )
    );
    $self->_transport->close;
}

# when the user calls a method on an object of this class, see if that method
# exists on the TCLIService object. If so, create a sub that calls that method
# on the client object. If not, die horribly.
sub AUTOLOAD {
    my ($self) = @_;
    ( my $meth = our $AUTOLOAD ) =~ s/.*:://;
    return if $meth eq 'DESTROY';
    print STDERR "$meth\n";
    no strict 'refs';
    if ( $self->_client->can($meth) ) {
        *$AUTOLOAD = sub { shift->_client->$meth(@_) };
        goto &$AUTOLOAD;
    }
    croak "No such method exists: $AUTOLOAD";
}

1;


