package Thrift::API::HiveClient2;

# ABSTRACT: Perl to HiveServer2 Thrift API wrapper

use strict;
use warnings;

use Moo;
use Carp;
use Scalar::Util qw( reftype blessed );

use Thrift;
use Thrift::Socket;
use Thrift::BufferedTransport;

# Protocol loading is done dynamically later.

use Thrift::API::HiveClient2::TCLIService;

# See https://msdn.microsoft.com/en-us/library/ms711683(v=vs.85).aspx
my @odbc_coldesc_fields = qw(
    TABLE_CAT
    TABLE_SCHEM
    TABLE_NAME
    COLUMN_NAME
    DATA_TYPE
    TYPE_NAME
    COLUMN_SIZE
    BUFFER_LENGTH
    DECIMAL_DIGITS
    NUM_PREC_RADIX
    NULLABLE
    REMARKS
    COLUMN_DEF
    SQL_DATA_TYPE
    SQL_DATETIME_SUB
    CHAR_OCTET_LENGTH
    ORDINAL_POSITION
    IS_NULLABLE
);

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
has sasl => (
    is      => 'ro',
    default => 0,
);

# 1 hour default recv socket timeout. Increase for longer-running queries
# called "timeout" for simplicity's sake, as this is how a user will experience
# it: a time after which the Thrift stack will throw an exception if not
# getting an answer from the server

has timeout => (
    is      => 'rw',
    default => sub { 3_600 },
);

# These exist to make testing with various other Thrift Implementation classes
# easier, eventually.

has _socket    => ( is => 'rwp', lazy => 1 );
has _transport => ( is => 'rwp', lazy => 1 );
has _protocol  => ( is => 'rwp', lazy => 1 );
has _client    => ( is => 'rwp', lazy => 1 );
has _sasl      => ( is => 'rwp', lazy => 1 );

# setters implied by the 'rwp' mode on the attrs above.

sub _set_socket    { $_[0]->{_socket}    = $_[1] }
sub _set_transport { $_[0]->{_transport} = $_[1] }
sub _set_protocol  { $_[0]->{_protocol}  = $_[1] }
sub _set_client    { $_[0]->{_client}    = $_[1] }

sub _set_sasl {
    my ($self, $sasl) = @_;
    return if !$sasl;

    # This normally selects XS first (hopefully)
    require Authen::SASL;
    Authen::SASL->import;

    require Thrift::SASL::Transport;
    Thrift::SASL::Transport->import;

    if ($sasl == 1) {
        return $self->{_sasl} = Authen::SASL->new( mechanism => 'GSSAPI' );
    }
    elsif (reftype $sasl eq "HASH") {
        return $self->{_sasl} = Authen::SASL->new( %$sasl ); #, debug => 8 );
    }
    die "Incorrect parameter passed to _set_sasl";
}

# after constructon is complete, initialize any attributes that
# weren't set in the constructor.
sub BUILD {
    my $self = shift;

    $self->_set_socket( Thrift::Socket->new( $self->host, $self->port ) )
        unless $self->_socket;

    $self->_set_sasl($self->sasl) if ( $self->sasl && !$self->_sasl );

    if ( !$self->_transport ) {
        my $transport = Thrift::BufferedTransport->new( $self->_socket );
        if ( $self->_sasl ) {
            $self->_set_transport( Thrift::SASL::Transport->new( $transport, $self->_sasl ) );
        }
        else {
            $self->_set_transport($transport);
        }
    }

    $self->_set_protocol( $self->_init_protocol( $self->_transport ) )
        unless $self->_protocol;

    $self->_set_client( Thrift::API::HiveClient2::TCLIServiceClient->new( $self->_protocol ) )
        unless $self->_client;
}


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


sub connect {
    my ($self) = @_;
    $self->_socket->setRecvTimeout($self->timeout * 1000);
    $self->_transport->open;
}

has _session => (
    is      => 'rwp',
    isa     => sub {
        die "Session isn't a Thrift::API::HiveClient2::TOpenSessionResp"
            if ! blessed($_[0]) || !$_[0]->isa('Thrift::API::HiveClient2::TOpenSessionResp') },
    lazy    => 1,
    builder => '_build_session',
);

has username => (
    is      => 'rwp',
    lazy    => 1,
    default => sub { $ENV{USER} },
);

has password => (
    is      => 'rwp',
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
    isa     => sub {
        die "Session handle isn't a Thrift::API::HiveClient2::TSessionHandle"
            if ! blessed($_[0]) || !$_[0]->isa('Thrift::API::HiveClient2::TSessionHandle') },
    lazy    => 1,
    builder => '_build_session_handle',
);

sub _build_session_handle {
    my $self = shift;
    return $self->_session->{sessionHandle};
}

has _operation => (
    is  => "rwp",
    isa => sub {
        die "Operation isn't a Thrift::API::HiveClient2::T*Resp"
            if defined $_[0]
            && (
            !blessed( $_[0] )
            || (   !$_[0]->isa('Thrift::API::HiveClient2::TExecuteStatementResp')
                && !$_[0]->isa('Thrift::API::HiveClient2::TGetColumnsResp') )
            );
    },
    lazy => 1,
);

has _operation_handle => (
    is => 'rwp',
    isa => sub {
        die
            "Operation handle isn't a Thrift::API::HiveClient2::TOperationHandle"
            if defined $_[0] && ( ! blessed($_[0]) || !$_[0]->isa('Thrift::API::HiveClient2::TOperationHandle') );
    },
    lazy    => 1,
);

sub _cleanup_previous_operation {
    my $self = shift;

    # We seeem to have some memory leaks in the Hive server, let's try freeing the
    # operation handle explicitely
    if ( $self->_operation_handle ) {
        $self->_client->CloseOperation(
             Thrift::API::HiveClient2::TCloseOperationReq->new(
                 { operationHandle => $self->_operation_handle, }
             )
        );
        $self->_set__operation(undef);
        $self->_set__operation_handle(undef);
    }
}


sub execute {
    my $self = shift;
    my ($query) = @_;    # make this a bit more flexible

    $self->_cleanup_previous_operation;

    my $rh = $self->_client->ExecuteStatement(
        Thrift::API::HiveClient2::TExecuteStatementReq->new(
            { sessionHandle => $self->_session_handle, statement => $query, confOverlay => {} }
        )
    );
    if ($rh->{status}{errorCode}) {
        die __PACKAGE__ . "::execute: $rh->{status}{errorMessage}; HQL was: \"$query\"";
    }
    $self->_set__operation($rh);
    $self->_set__operation_handle($rh->{operationHandle});
    return $rh;
}

{
    # cache the column names we need to extract from the bloated data structure
    # (keyed on query)
    my $column_keys;

    sub fetch {
        my $self = shift;
        my ( $rv, $rows_at_a_time ) = @_;

        # if $rv looks like a number, use it instead of $rows_at_a_time
        $rows_at_a_time = $rv if !$rows_at_a_time && $rv =~ /^[1-9][0-9]*$/;

        my $result = [];
        my $has_more_rows;

        # NOTE we don't use $rv now, maybe we should leave that possibility open
        # for parallel queries, but that woudl need a lot more testing. Patches
        # welcome.
        my $rh = $self->_client->FetchResults(
            Thrift::API::HiveClient2::TFetchResultsReq->new(
                {   operationHandle => $self->_operation_handle,
                    maxRows         => $rows_at_a_time || 10_000
                }
            )
        );
        if ( ref $rh eq 'Thrift::API::HiveClient2::TFetchResultsResp' ) {

            # NOTE that currently (july 2013) the hasMoreRows method is broken,
            # see the explanation in the POD
            $has_more_rows = $rh->hasMoreRows();

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
                    [
                        map  { $_->value  }
                        grep { defined $_ }
                        map  { $row->{colVals}[ $idx++ ]{$_} }
                        @{ $column_keys->{$rv} }
                    ];
            }
        }
        return wantarray ? ( $result, $has_more_rows ) : ( @$result ? $result : undef );
    }
}

sub get_columns {
    my $self = shift;
    my ( $table, $schema ) = @_;

    # note that not specifying a table name would return all columns for all
    # tables we probably don't want that, but feel free to change this
    # behaviour. Same goes for the schema name: we probably want a default
    # value for the schema, which is what we use here.
    die "Unspecified table name" if !$table;
    $schema //= "default";

    $self->_cleanup_previous_operation;

    my $rh = $self->_client->GetColumns(
        Thrift::API::HiveClient2::TGetColumnsReq->new(
            {   sessionHandle => $self->_session_handle,
                catalogName   => undef,
                schemaName    => $schema,
                tableName     => $table,
                columnName    => undef,
                confOverlay   => {}
            }
        )
    );
    if ( $rh->{status}{errorCode} ) {
        die __PACKAGE__ . "::execute: $rh->{status}{errorMessage}";
    }
    $self->_set__operation($rh);
    $self->_set__operation_handle( $rh->{operationHandle} );
    my $columns;
    while ( my $res = $self->fetch($rh) ) {
        for my $line (@$res) {
            my $idx = 0;
            push @$columns, { map { $_ => $line->[ $idx++ ] } @odbc_coldesc_fields };
        }
    }
    return $columns;
}

sub DEMOLISH {
    my $self = shift;

    $self->_cleanup_previous_operation;

    if ( $self->_session_handle ) {
        $self->_client->CloseSession(
            Thrift::API::HiveClient2::TCloseSessionReq->new(
                { sessionHandle => $self->_session_handle, }
            )
        );
    }
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

__END__

=pod


=head1 METHODS

=head2 new

Initialize the client object with the Hive server parameters

    my $client = Thrift::API::HiveClient2->new(
        host    => <host name or IP, defaults to localhost>,
        port    => <port, defaults to 10000>,
        timeout => <seconds timeout, defaults to 1 hour>,
    );

=head2 connect

Open the connection on the server declared in the object's constructor.

     $client->connect() or die "Failed to connect";

=head2 execute

Run an HiveQl statement on an open connection.

    my $rh = $client->execute( <HiveQL statement> );

=head2 fetch

Returns an array(ref) of arrayrefs, like DBI's fetchall_arrayref, and a boolean
indicator telling wether or not a subsequent call to fetch() will return more
rows.

    my ($rv, $has_more_rows) = $client->fetch( $rh, <maximum records to retrieve> );

IMPORTANT: The version of HiveServer2 that we use for testing is the one
bundled with CDH 4.2.1. The hasMoreRows method is currently broken, and always
returns false. So the right way of obtaining the resultset is to keep using
fetch() until it returns an empty array. For this reason the behaviour of fetch
has been altered in scalar context (which becomes the current advised way of
retrieving the data):

    # $rv will be an arrayref is anything was fetched, and undef otherwise.
    #
    while (my $rv = $client->fetch( $rh, <maximum records to retrieve> )) {
        # ... do something with @$rv
    }

This is the approach adopted in
L<https://github.com/cloudera/hue/blob/master/apps/beeswax/src/beeswax/server/hive_server2_lib.py>

Starting with version 0.12, we cache the operation handle and don't need it as
a first parameter for the fetch() call. We want to be backward-compatible
though, so depending on the type of the first parameter, we'll ignore it (since
we cached it in the object and we can get it from there) or we'll use it as the
number of rows to be retrieved if it looks like a positive integer:

     my $rv = $client->fetch( 10_000 );

=head2 get_columns

Get the columns description for a table, returned in an array of hashrefs which keys are named after the result of an
 ODBC GetColumns call. "default" is used for the schema name is none is specified as 2nd argument.

    my $columns = $client->get_columns('<table name>'[, '<schema name>']);

=head1 WARNING

Thrift in Perl originally did not support SASL, so authentication needed to be
disabled on HiveServer2 by setting this property in your
/etc/hive/conf/hive-site.xml. Although the property is documented, this *value*
-which disables the SASL server transport- is not, AFAICT.

    <property>
      <name>hive.server2.authentication</name>
      <value>NOSASL</value>
    </property>

Starting with 0.014, support for secure clusters has been added thanks to
Thrift::SASL::Transport. This behaviour is set by passing sasl => 1 to the
constructor. It has been tested with hive.server2.authentication = KERBEROS.
It of course requires a valid credentials cache (kinit) or keytab.

Starting with 0.015, other authentication methods are supported, and driven by
the content of the sasl property. When built using sasl => 0 or sasl => 1, the
behaviour is unchanged. When passed a hashref of arguments that follow the
L<Authen::SASL> syntax for object creation, it is passed directly to
Authen::SASL, for instance:

    {
      mechanism  => 'PLAIN',
      callback   => {
        canonuser => $USER, # not 'user', as I thought reading Authen::SASL's doc
        password  => "foobar",
      }
    }

Note that a server configured with NONE will happily accept the PLAIN method.

=head1 CAVEATS

The instance of hiveserver2 we have didn't return results encoded in UTF8, for
the reason mentioned here:
L<https://groups.google.com/a/cloudera.org/d/msg/cdh-user/AXeEuaFP0Ro/Txmn1OHleAsJ>

So we had to change the init script for hive-server2 to make it behave, adding
'-Dfile.encoding=UTF-8' to HADOOP_OPTS

=head1 CONTRIBUTORS

Burak Gürsoy (BURAK)

=cut
