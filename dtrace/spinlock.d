BEGIN
{
   end = timestamp + 2 * 1000000000;
   created = 0;
   destroyed = 0;
   fastspin = 0;
}

tick-10hz
/timestamp >= end/
{
   printf("\nCreated  : %d\nDestroyed: %d\nFastspin : %d\n",
          created, destroyed, fastspin);

   exit(0);
}

ep*:::spinlock-created
{
   created += 1;
}

ep*:::spinlock-destroyed
{
   destroyed += 1;
}

ep*:::spinlock-acquired / arg1 != 0 /
{
   @spins = quantize(arg1);
   @spinlock[arg0] = count();
   @stack[ustack(6)] = count();
}

ep*:::spinlock-acquired / arg1 == 0 /
{
   fastspin += 1;
}
