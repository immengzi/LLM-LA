---
id: c01-048
date:
campaign: 01-push-vs-pull-strategy
series: "48"
exp_ids: [193, 194, 195, 196, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253]
methods: []
status: migrated
source: logs
---
# Series 48 — series 48

Series 48:
Setup:
- 24 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 40000
- pattern: "det"
- rate_rps: 14
- Methods:
  193) Pull
  194) Push-RR
  195) Push-Random
  196) Push-Least-Queue

Purpose: TODO
Findings: TODO


Metric (mean / p99)            | Pull (exp193) | Push-RR (exp194)        | Push-Random (exp195)     | Push-LQ (exp196)         | Winner
-------------------------------|---------------|--------------------------|---------------------------|---------------------------|--------
↓ End-to-end latency mean (s)  | 20.43         | 19.52 (−4.49%)           | 17.22* (−15.72%)          | 18.93 (−7.35%)            | Push-Random
↓ End-to-end latency p99 (s)   | 247.67        | 232.27 (−6.22%)          | 200.48* (−19.05%)         | 234.78 (−5.20%)           | Push-Random

↑ Decode TPS mean              | 3531.58*      | 2566.68 (−27.32%)        | 2575.18 (−27.08%)         | 2586.53 (−26.76%)         | Pull
↑ Prefill TPS mean             | 499.70*       | 362.53 (−27.46%)         | 364.36 (−27.08%)          | 367.74 (−26.41%)          | Pull

↑ Requests running mean        | 114.10*       | 77.85 (−31.77%)          | 70.09 (−38.57%)           | 76.37 (−33.07%)           | Pull

↓ TTFT mean (s)                | 25.72         | 21.96 (−14.63%)          | 14.41* (−43.97%)          | 19.91 (−22.58%)           | Push-Random
↓ TPOT mean (s)                | 0.7803        | 0.7181 (−7.97%)          | 0.6460* (−17.21%)         | 0.7032 (−9.88%)           | Push-Random


Metric                         | Pull (exp193) | Push-RR (exp194)        | Push-Random (exp195)      | Push-LQ (exp196)         | Winner
-------------------------------|---------------|--------------------------|----------------------------|---------------------------|--------
↑ Successful requests          | 38851*        | 36863 (−5.12%)           | 5305 (−86.35%)             | 27119 (−30.20%)           | Pull
↓ Connection failures          | 1149*         | 3137 (+173.02%)          | 34695 (+2920.71%)          | 12881 (+1020.97%)         | Pull

↑ Success rate (%)             | 97.1275*      | 92.1575 (−5.12%)         | 13.2625 (−86.35%)          | 67.7975 (−30.20%)         | Pull
↓ Failure rate (%)             | 2.8725*       | 7.8425 (+173.02%)        | 86.7375 (+2920.71%)        | 32.2025 (+1020.97%)       | Pull


201-225
300 requests experriments

226-238
10000 requests experiments on 6 rps

241-253
Setup:
- 16 servers
- No cap on input tokens
- Request response length: 8192
- Total requests: 10000
- pattern: "det"
- rate_rps: 5
- Methods:
  193) Pull
  194) Push-RR
  195) Push-Random
  196) Push-Least-Queue



Metric (mean / p99)            | pull (exp241)     | push-rr (exp242)     | push-random (exp243) | push-lq (exp244)     | random (exp245)      | least-req (exp246)   | throughput (exp247)  | prefix-cache (exp248) | least-busy (exp249)  | least-kv (exp250)    | least-lat (exp251)   | prefix-preble (exp252) | vtc-basic (exp253)   | Winner
-------------------------------|-------------------|----------------------|----------------------|----------------------|----------------------|----------------------|----------------------|-----------------------|----------------------|----------------------|----------------------|------------------------|----------------------|-------
↓ E2E latency mean (s)         | 25.75*            | 85.07 (+230.4%)      | 57.09 (+121.7%)      | 69.22 (+168.8%)      | 78.90 (+206.4%)      | 30.53 (+18.5%)       | 131.03 (+408.9%)     | 125.48 (+387.3%)      | 85.34 (+231.4%)      | 99.24 (+285.4%)      | 130.33 (+406.1%)     | 134.52 (+422.4%)       | 142.10 (+451.9%)     | pull
↓ E2E latency p99 (s)          | 299.74            | 353.27 (+17.9%)      | 319.90 (+6.7%)       | 324.55 (+8.3%)       | 609.89 (+103.5%)     | 278.99* (−6.9%)      | 573.23 (+91.2%)      | 401.81 (+34.1%)       | 470.38 (+56.9%)      | 377.06 (+25.8%)      | 475.75 (+58.7%)      | 527.72 (+76.1%)        | 484.04 (+61.5%)      | least-req

↑ Decode TPS mean              | 2697.94           | 2433.49 (−9.8%)      | 2481.18 (−8.0%)      | 2521.03 (−6.6%)      | 2179.16 (−19.2%)     | 2726.58* (+1.1%)     | 2194.15 (−18.7%)     | 2453.91 (−9.0%)       | 2404.68 (−10.9%)     | 2491.89 (−7.6%)      | 2317.79 (−14.1%)     | 2276.59 (−15.6%)       | 2259.78 (−16.2%)     | least-req
↑ Prefill TPS mean             | 382.13            | 345.58 (−9.6%)       | 351.14 (−8.1%)       | 356.68 (−6.7%)       | 308.86 (−19.2%)      | 387.53* (+1.4%)      | 311.92 (−18.4%)      | 347.62 (−9.0%)        | 339.67 (−11.1%)      | 353.90 (−7.4%)       | 328.90 (−14.0%)      | 322.86 (−15.5%)        | 319.69 (−16.4%)      | least-req

↑ Requests running mean        | 101.98            | 99.80 (−2.1%)        | 91.09 (−10.7%)       | 100.45 (−1.5%)       | 82.93 (−18.7%)       | 105.97 (+3.9%)       | 91.27 (−10.5%)       | 109.85* (+7.7%)       | 93.68 (−8.1%)        | 101.81 (−0.2%)       | 98.12 (−3.8%)        | 96.59 (−5.3%)          | 94.66 (−7.2%)        | prefix-cache

↓ TTFT mean (s)                | 1.26*             | 1.36 (+8.1%)         | 1.11 (−11.9%)        | 1.24 (−1.4%)         | 166.74 (+13130.8%)   | 7.88 (+525.1%)       | 162.00 (+12760.5%)   | 105.27 (+8252.7%)     | 93.13 (+7291.5%)     | 90.27 (+7065.5%)     | 130.54 (+10254.5%)   | 160.18 (+12601.1%)     | 144.77 (+11381.0%)   | pull
↓ TPOT mean (s)                | 0.0384*           | 0.0416 (+8.3%)       | 0.0360 (−6.3%)       | 0.0404 (+5.1%)       | 0.0400 (+4.2%)       | 0.0406 (+5.6%)       | 0.0431 (+12.2%)      | 0.0475 (+23.8%)       | 0.0397 (+3.4%)       | 0.0422 (+9.8%)       | 0.0436 (+13.5%)      | 0.0458 (+19.2%)        | 0.0430 (+12.0%)      | push-random



254-260
40000 requests experiments on 5 rps - aibrix methods until throughput method - should be re-done as the least-request

261:

Boom -> llm-la
GLM-5
replica 2
batch: 8
rps: 1

262:

Boom -> llm-la
GLM-5
replica 2
batch: 16
rps: 1

263:

Boom -> llm-la
GLM-5
replica 2
batch: 32
rps: 1

264:

Boom
GLM-5
replica 2
batch: 8
rps: 1

265:

Boom
GLM-5
replica 2
batch: 16
rps: 1

266:

Boom
GLM-5
replica 2
batch: 32
rps: 1

267:
corrupted just saved for errro long

268:
boom+llm-la
GLM-5
replica 4
batch: 8
rps: 2
10000

269:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom+llm-la
GLM-5
replica 4
batch: 16
rps: 2
10000

270:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom+llm-la
GLM-5
replica 4
batch: 32
rps: 2
10000

271:
boom+llm-la
GLM-5
replica 4
batch: 8
rps: 2.5
10000

272:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom+llm-la
GLM-5
replica 4
batch: 16
rps: 2.5
10000

273:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom+llm-la
GLM-5
replica 4
batch: 32
rps: 2.5
10000

274:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom
GLM-5
replica 4
batch: 8
rps: 2
10000

275:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom
GLM-5
replica 4
batch: 16
rps: 2
10000

276:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom
GLM-5
replica 4
batch: 32
rps: 2
10000

277:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom
GLM-5
replica 4
batch: 8
rps: 2.5
10000

278:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom
GLM-5
replica 4
batch: 16
rps: 2.5
10000

279:
The batch variable is being
controled through the helm var rather than ours not sure which one was taking effect here
boom
GLM-5
replica 4
batch: 32
rps: 2.5
10000

280-282
only llm-la sanity check experiments with rps of 2 and batch of 2, 4, 8,16

283-289
only llm-la sanity check experiments with rps of 2,4,8,16 and batch size of 8 for both pull and push-rr method (as a substitute of boom push-rr)

290-292

repeat of 284-287

288-299

TODO should check

301-302

300 requests
good results

303-304

2000 requests
good results - best comparison with Boom so far

305-306

5000 requests
good results - the benefit is marginalized
becasuse of the requests getting shorter towrad the end


Questions:

1. what is your target metric to optimize?
2. What is the metric to define the imbalance?
3. what is the load pattern?
4. What is the dataset and is it sharable?
5. What are the software stack and models?
6. Comparison with base frameworks?
7. LiteLLM
