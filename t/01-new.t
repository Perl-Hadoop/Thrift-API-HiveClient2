use 5.006;
use strict;
use warnings FATAL => 'all';
use Test::More;

use Data::Dumper;
use Thrift::API::HiveClient2;

my $HIVEHOST = $ENV{HIVEHOST};
my $HIVEPORT = $ENV{HIVEPORT} || 10_000;

plan skip_all => "Set up HiveServer2 host with \$ENV{HIVEHOST}" 
    if !$HIVEHOST;
plan tests => 6;

my $obj = Thrift::API::HiveClient2->new(
                host => $HIVEHOST,
                port => $HIVEPORT,
            );

ok( ref( $obj ) =~ /Thrift/, "Default client" );

SKIP: {
    skip "connect: set HIVEHOST (and optionally HIVEPORT) environment variable(s)", 1
        if !$HIVEHOST;
    ok( eval { $obj->connect }, "Connecting to server");
}

my $obj_no_xs = Thrift::API::HiveClient2->new(
                    host   => $HIVEHOST,
                    port   => $HIVEPORT,
                    use_xs => 0,
                );

ok( ref( $obj_no_xs ) !~ /XS/, "Client with XS disabled" );

SKIP: {
    skip "connection test", 1 if !$HIVEHOST;
    ok( eval { $obj_no_xs->connect }, "Connecting to server");
}

SKIP: {
    skip "Test is disabled. Thrift::XS::BinaryProtocol non functional with BufferedTransport", 2;

    if ( ! eval { require Thrift::XS::BinaryProtocol } ) {
        skip "Thrift::XS::BinaryProtocol not installed", 2;
    }

    my $obj_xs = Thrift::API::HiveClient2->new(
                    host   => $HIVEHOST,
                    port   => $HIVEPORT,
                    use_xs => 1
                );

    ok( ref($obj_xs ) =~ /XS/, "Client with XS enabled" );
    ok( eval { $obj_no_xs->connect }, "Connecting to server");
}

#test_execution($obj);

sub test_execution {
    my $client = shift || die "No client?";
    eval {
        my $handle = $client->execute('SHOW TABLES');
        print Dumper($client->fetch($handle, 10));
        1;
    } or do {
        my $eval_error = $@ || 'Zombie error';
        diag "Failed to execute: " . Dumper $eval_error;
    };
}