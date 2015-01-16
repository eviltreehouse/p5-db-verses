package Verses::Plan;
use strict;

sub new {
	return bless({}, $_[0]);
}

sub up {
	print "no up.";
}

sub down {
	print "no down.";
}

sub db {
	return Verses::db_handle();
}


1;