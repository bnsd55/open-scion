The stream-slot quota test's reply grew from 16 MiB to 64 MiB: the
pipeline between the producer and a client that reads nothing can soak
up about 4 MiB of userspace plus up to about 10 MB of loopback TCP
autotuning, and a runner on the fat end of that absorbed the whole
16 MiB reply, freed the slot before the probe, and the test saw
NotFound where it wanted ResourceExhausted. 64 MiB clears every buffer
a CI runner can tune; the quota bounds server work, not bytes in
flight, so the slot is right to free once the producer hands its last
byte off, and the test owns the margin.
