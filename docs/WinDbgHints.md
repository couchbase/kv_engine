# Hints on how to debug a minidump file with WinDbg

You need to install the Couchbase server on the machine, and extract
the PDB zip file (I've put it in C:\Symbols). After installing the
server you might want to stop (and disable) the Couchbase service.

Open WinDbg (64 bit); if you don't have it available on your machine
you can get it as part of the Windows SDK (I failed to install the
standalone package). With the minidump file loaded in WinDbg you need
to tell it how to locate the PDB file for memcached:

    .sympath+ C:\Symbols\couchbase-server-enterprise_7.6.4-windows_amd64-PDB\build\kv_engine

Then tell it to reload the information

    .reload /f

With all the symbol information available it is time to analyze the crash:

    !analyze -v

And you may fetch the callstack for all threads

    ~*k

Then save the output (there is an option to save the content in the
menu) and feed it to Gemini.