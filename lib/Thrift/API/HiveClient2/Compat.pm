package Thrift::API::HiveClient2::Compat;

use strict;
use warnings;
use 5.010;

use Thrift;

# Compatibility layer for the changes in the new versions.
#
# To be removed in the near future to break free from older releases of Thrift.

BEGIN {
    if ( ! defined &TType::STOP ) {
        # >= 0.11.0
        # Yes, the naming is broken in Thrift:: since forever
        require Thrift::Type;
        require Thrift::MessageType;
        require Thrift::Exception;
        *TType::                 = *Thrift::TType::;
        *TMessageType::          = *Thrift::TMessageType::;
        *TApplicationException:: = *Thrift::TApplicationException::;
    }
}

# More things to consider
# [1]
# eval { require Thrift::SSLSocket; } or do { require Thrift::Socket; }
# [2]
# Thrift::HttpClient setRecvTimeout() and setSendTimeout() are deprecated.
# Use setTimeout instead.

1;

__END__
