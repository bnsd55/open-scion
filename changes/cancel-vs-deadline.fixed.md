A call whose client went now answers cancelled, not deadline_exceeded.
A job that outlives its call checks whether the call has gone: it stopped
because the caller went, so the reply maps to Status::cancelled, and only
a call that ran past its deadline reads deadline_exceeded. A land whose
call went stays pending, and its counter says cancelled, not internal.
