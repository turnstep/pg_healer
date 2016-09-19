/* pg_healer/pg_healer--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_healer" to load this file. \quit

CREATE FUNCTION pg_healer_corrupt(regclass, text)
RETURNS int
LANGUAGE C STRICT
AS 'MODULE_PATHNAME';

CREATE FUNCTION pg_healer_remove_from_buffer(regclass)
RETURNS int
LANGUAGE C STRICT
AS 'MODULE_PATHNAME';

CREATE TABLE pg_healer_config (
  name TEXT,
  setting TEXT
);

CREATE TABLE pg_healer_log (
  action TEXT NOT NULL,
  target TEXT NOT NULL,
  ctime  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION pg_healer_cauldron() RETURNS TEXT LANGUAGE plperlu AS $$

use strict;
use warnings;
use autodie;
use Cwd;

use vars qw/$debug $verbose $pgadir $lastread/;

$debug = 0;
$verbose = 1;

my $datadir = getcwd;
my $oldbase = "$datadir/base";
-e $oldbase or die qq{Are you sure "$datadir" is a data directory?!\n};

$pgadir = "$datadir/pg_healer";
if (! -e $pgadir) {
  mkdir $pgadir, 0700;
}

## See if we can figure out the last time we started a sync
my $readme = "$pgadir/README";
my $oldtimes = '';
$lastread = 0;
if (-e $readme) {
   open my $fh, '<', $readme;
   { local $/; $oldtimes = <$fh>; }
   if ($oldtimes =~ /STARTED: (\d+)/) {
       $lastread = $1;
   }
   close $fh;
}
$debug and print "Lastread is $lastread\n";
## Found or not, write a new timestamp at the top
open my $fh, '>', $readme;
printf {$fh} "STARTED: %d %s\n%s", time, (scalar localtime), $oldtimes;
close $fh;

## Walk the old ones - recursing as we go and copying things over
walker($datadir, 'base');

sub walker {

    ## Walk a directory looking for files of interest to copy
    my $basedir = shift or die;
    my $dirname = shift or die;
    my $fullpath = "$basedir/$dirname";
    opendir my ($dh), $fullpath;

    while (readdir $dh) {
        my $file = "$fullpath/$_";
        my $type = -d $file ? 'dir' : -f $file ? 'file' : '???';
        if ('dir' eq $type) {
            if ($_ =~ /^\d+$/) {
                ## Make sure this remote directory exists before we descend into it
                for my $remotedir ("$pgadir/$dirname", "$pgadir/$dirname/$_") {
                    if (! -e $remotedir) {
                        mkdir $remotedir, 0700;
                        $debug and print "Created $remotedir\n";
                    }
                }
                walker($basedir, "$dirname/$_");
            }
        }
        if ('file' eq $type) {
            ## It if ends in just a number, we want to consider it
            if ($file =~ /\d+$/) {
                ## Does it exist on the remote side?
                my $remotefile = "$pgadir/$dirname/$_";
                my $copyit = 0;
                ## See if it is possible to skip this one
                if (-e $remotefile) {
                    ## Grab last modification time
                    my $mtime = (stat $file)[9];
                    ## If this is absolutely older than the last run, we can ignore it
                    if ($mtime < $lastread) {
                        $debug and print "File $file is too old, skipping ($mtime < $lastread)\n";
                        next;
                    }
                    ## If this is older than the actual file mod time, we can ignore it
                    my $remote_mtime = (stat $remotefile)[9];
                    if ($mtime < $remote_mtime) {
                        $debug and print "File $file is too ol', skipping ($mtime < $remote_mtime)\n";
                        next;
                    }
                }

                ## As a final check, only copy the file if the checksum is already valid
                ## Copy to temp file, check *that*, and move over as needed
                my $remotefiletmp = "$remotefile.tmp";
                my $COM = "cp -f $file $remotefiletmp";
                system ($COM);
                $debug and print "Copied $file to $remotefiletmp\n";

                ## Are all the checksums in this file valid?
                my $checksums_valid = 1; ## TODO, but actually easier in C
                if (! $checksums_valid) {
                    unlink $remotefiletmp;
                    print "File $file could not be copied, as it contains checksum failures\n";
                    next;
                }

                $COM = "mv -f $remotefiletmp $remotefile";
                system ($COM);
                $verbose and print "Moved $remotefiletmp to $remotefile\n";
            }
        }
    }
}

$$;
