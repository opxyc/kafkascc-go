import { z } from "zod";

export const FedMobileEvent = z.any();

export type MobEvent = z.infer<typeof FedMobileEvent>;
