/* Copyright (C) 2004 Hewlett-Packard Co.
     Contributed by David Mosberger-Tang <davidm@hpl.hp.com>.

This file is part of libunwind.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  */

#define oR8  "0x28"
#define oR9  "0x30"
#define oR10 "0x38"
#define oR11 "0x40"
#define oR12 "0x48"
#define oR13 "0x50"
#define oR14 "0x58"
#define oR15 "0x60"
#define oRDI "0x68"
#define oRSI "0x70"
#define oRBP "0x78"
#define oRBX "0x80"
#define oRDX "0x88"
#define oRAX "0x90"
#define oRCX "0x98"
#define oRSP "0xa0"
#define oRIP "0xa8"
#define oFPREGS  "0x1a8"
#define oFPREGSMEM  "0xe0"
#define UC_SIGMASK    "0x128"
#define oMXCSR "0x18"
