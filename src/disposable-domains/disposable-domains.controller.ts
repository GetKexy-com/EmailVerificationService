import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { SkipThrottle } from '@nestjs/throttler';

import { DisposableDomainsService } from '@/disposable-domains/disposable-domains.service';
import { DisposableDomain } from '@/disposable-domains/entities/disposable-domain.entity';

@Controller('disposable-domains')
export class DisposableDomainsController {
  constructor(
    private readonly disposableDomainsService: DisposableDomainsService,
  ) {}

  @SkipThrottle()
  @Post('/create-many')
  async createMany(@Body() disposableDomains: DisposableDomain[]) {
    return await this.disposableDomainsService.createMany(disposableDomains);
  }

  @Get(':domain')
  findOne(@Param('domain') domain: string) {
    return this.disposableDomainsService.findByDomain(domain);
  }
}
