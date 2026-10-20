Waits bound by a call's deadline read what is left from one method,
Deadline::remaining, instead of each call site computing it by hand;
after the deadline it is zero, never a panic.
