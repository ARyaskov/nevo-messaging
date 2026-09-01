import { DynamicModule, Global, Inject, Module, OnApplicationShutdown, OnModuleInit, Optional, Provider, Type } from "@nestjs/common"
import { DiscoveryModule, DiscoveryService } from "@nestjs/core"
import { bindNevoRouter, disposeNevoRouter, getRouterClassMetadata, type NevoRuntimeOptions } from "./router-runtime"
import { getDefaultLogger } from "./common/logger"

export const NEVO_RUNTIME_OPTIONS = "NEVO_RUNTIME_OPTIONS"
export const NEVO_FEATURE_OPTIONS = "NEVO_FEATURE_OPTIONS"

export interface NevoModuleAsyncOptions {
  imports?: DynamicModule["imports"]
  inject?: any[]
  useFactory: (...args: any[]) => NevoRuntimeOptions | Promise<NevoRuntimeOptions>
}

/** Later sources win per key; values are replaced wholesale, not deep-merged. */
function mergeRuntimeOptions(...sources: (NevoRuntimeOptions | undefined)[]): NevoRuntimeOptions {
  const out: NevoRuntimeOptions = {}
  for (const source of sources) {
    if (!source) continue
    for (const [key, value] of Object.entries(source)) {
      if (value === undefined) continue
      ;(out as Record<string, unknown>)[key] = value
    }
  }
  return out
}

/** Wires every `@*SignalRouter` controller to a pipeline built from DI, via discovery. */
@Global()
@Module({ imports: [DiscoveryModule] })
export class NevoModule implements OnModuleInit, OnApplicationShutdown {
  private readonly bound: Type<any>[] = []

  constructor(
    @Inject(DiscoveryService) private readonly discovery: DiscoveryService,
    @Inject(NEVO_RUNTIME_OPTIONS) private readonly rootOptions: NevoRuntimeOptions,
    @Optional() @Inject(NEVO_FEATURE_OPTIONS) private readonly featureOptions?: NevoRuntimeOptions
  ) {}

  static forRoot(options: NevoRuntimeOptions = {}): DynamicModule {
    return {
      module: NevoModule,
      providers: [{ provide: NEVO_RUNTIME_OPTIONS, useValue: options }],
      exports: [NEVO_RUNTIME_OPTIONS]
    }
  }

  static forRootAsync(async: NevoModuleAsyncOptions): DynamicModule {
    return {
      module: NevoModule,
      imports: async.imports ?? [],
      providers: [
        {
          provide: NEVO_RUNTIME_OPTIONS,
          inject: async.inject ?? [],
          useFactory: async.useFactory
        }
      ],
      exports: [NEVO_RUNTIME_OPTIONS]
    }
  }

  static forFeature(options: NevoRuntimeOptions): Provider {
    return { provide: NEVO_FEATURE_OPTIONS, useValue: options }
  }

  static forFeatureAsync(async: NevoModuleAsyncOptions): Provider {
    return { provide: NEVO_FEATURE_OPTIONS, inject: async.inject ?? [], useFactory: async.useFactory }
  }

  private featureOverrideFor(wrapper: unknown): NevoRuntimeOptions | undefined {
    const host = (wrapper as { host?: { getProviderByKey?: (token: unknown) => { instance?: unknown } | undefined } }).host
    const provider = host?.getProviderByKey?.(NEVO_FEATURE_OPTIONS)
    const instance = provider?.instance
    return instance && typeof instance === "object" ? (instance as NevoRuntimeOptions) : undefined
  }

  onModuleInit(): void {
    const baseLogger = this.rootOptions.logger ?? getDefaultLogger().child({ component: "nevo-module" })

    for (const wrapper of this.discovery.getControllers()) {
      const ctor = wrapper.metatype as Type<any> | undefined
      if (!ctor || typeof ctor !== "function") continue
      if (!getRouterClassMetadata(ctor)) continue
      const options = mergeRuntimeOptions(this.rootOptions, this.featureOptions, this.featureOverrideFor(wrapper))
      const runtime = bindNevoRouter(ctor, options)
      this.bound.push(ctor)
      baseLogger.debug({ event: "nevo.router_bound", controller: ctor.name, service: runtime.serviceName, topic: runtime.eventPattern })
    }

    if (this.bound.length === 0) {
      baseLogger.warn(
        { event: "nevo.no_routers" },
        "NevoModule found no signal-router controllers. Declare them in a module that imports NevoModule, or drop the import."
      )
    }
  }

  async onApplicationShutdown(): Promise<void> {
    for (const ctor of this.bound) await disposeNevoRouter(ctor)
    this.bound.length = 0
  }
}
