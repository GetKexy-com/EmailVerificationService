import { SetMetadata } from '@nestjs/common';

export const IS_PUBLIC_KEY = 'isPublic';

export const Public = () => {
  // Used in api-key.guard.ts
  return SetMetadata(IS_PUBLIC_KEY, true);
};
