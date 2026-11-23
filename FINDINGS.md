# In-Class Group Activity 4: Managing Concurrency Using Scala Futures

## Group Information
- **Group Members:** Tucker M. (Ttmoose), B. Hansen (Bhansen9) <!-- Update/add group member names as needed -->
- **Repository URL:** [https://github.com/Bhansen9/In-Class-Group-Activity-4-Concurrency](https://github.com/Bhansen9/In-Class-Group-Activity-4-Concurrency)

---

## 1. Initial Web Client Exploration with `curl`

Before running the Scala code, we probed the prime-checking microservices using `curl -s -i` across different service instances and numbers.

### Services Tested
1. `http://laufer-luc-oci.privatedns.org:8080/`
2. `http://linode5.cs.luc.edu:8080/`
3. `https://primenumbers-http4s-scala-latest.onrender.com/`

### Test Cases and Observed Outputs

#### Case 1: Large Prime `10000000000391`
```bash
curl -s -i https://primenumbers-http4s-scala-latest.onrender.com/10000000000391
```
**Response:**
```http
HTTP/1.1 200 OK
Date: Mon, 23 Nov 2026 22:36:23 GMT
Content-Type: text/plain; charset=UTF-8
Server: cloudflare
x-render-origin-server: Render

Yay, 10000000000391 happens to be a prime!
```

#### Case 2: Small Prime `6007`
```bash
curl -s -i https://primenumbers-http4s-scala-latest.onrender.com/6007
```
**Response:**
```http
HTTP/1.1 200 OK
Content-Type: text/plain; charset=UTF-8

Yay, 6007 happens to be a prime!
```

#### Case 3: Non-Prime / Composite `999989`
```bash
curl -s -i https://primenumbers-http4s-scala-latest.onrender.com/999989
```
**Response:**
```http
HTTP/1.1 404 Not Found
Content-Type: text/plain; charset=UTF-8

Bummer, 999989 is not a prime.
```

#### Case 4: Very Large Prime `9927935178558959`
```bash
curl -s -i https://primenumbers-http4s-scala-latest.onrender.com/9927935178558959
```
**Response:**
```http
HTTP/1.1 200 OK
Content-Type: text/plain; charset=UTF-8

Yay, 9927935178558959 happens to be a prime!
```

### Key Protocol Observations
- **Prime Numbers**: Return HTTP status `200 OK` with body `Yay, <n> happens to be a prime!`.
- **Non-Prime Numbers**: Return HTTP status `404 Not Found` with body `Bummer, <n> is not a prime.`.
- **Render Service Latency**: The Render-hosted microservice experiences cold starts ("spin up" delay) after inactivity. The first request may take several seconds or experience a handshake timeout before responding.

---

## 2. Analysis & Answers to Discussion Questions

### Question 1: Concisely speaking, what does this code do?

**Answer:**
`WebServiceClient` is a concurrent, non-blocking HTTP client designed to count the number of primes within a specified integer range `[start, end]`. It achieves this by:
1. Generating a collection of URLs for numbers in the range `[start, end]`.
2. Dispatching asynchronous HTTP GET requests concurrently to a prime-checking microservice via an asynchronous HTTP client (`StandaloneAhcWSClient`).
3. Mapping the asynchronous responses into boolean values (`true` if status is `200 OK`, `false` otherwise).
4. Aggregating all individual asynchronous operations into a single future using `Future.sequence`.
5. Counting the total number of primes (`true` values) found across the range in the background without blocking the main execution thread.
6. Providing both a standard aggregate (`f1` / `f2`) and a resilient, failsafe aggregate (`f3` / `f4` using `.recover`) that can tolerate network connection errors or timeouts.
7. Providing inspection methods (`printProgress` and `printResults`) so the user can observe progress and partial/final results in the foreground while requests process in the background.

---

### Question 2: What are the types of `urls`, `requests`, `results`, `f1`, and `f2`?

| Variable Name | Exact Scala Type | Conceptual Explanation |
| :--- | :--- | :--- |
| `urls` | `scala.collection.immutable.IndexedSeq[String]` | A collection of URL strings constructed by appending each integer in `start to end` to the base service URL. |
| `requests` | `scala.collection.immutable.IndexedSeq[Future[StandaloneAhcWSResponse]]` | A sequence of pending asynchronous HTTP responses returned by `wsClient.url(u).get()`. |
| `results` | `scala.collection.immutable.IndexedSeq[Future[Boolean]]` | A sequence of futures where each HTTP response has been mapped to `Boolean` (`r.status == 200`). |
| `resultsFailsafe` | `scala.collection.immutable.IndexedSeq[Future[Boolean]]` | Same as `results`, but with `.recover` catching network exceptions and substituting `false`. |
| `f1` | `scala.concurrent.Future[scala.collection.immutable.IndexedSeq[Boolean]]` | A single future wrapping a sequence of booleans, produced by `Future.sequence(results)`. |
| `f2` | `scala.concurrent.Future[Int]` | A single future containing the final integer count of primes, produced by `f1.map(r => r.count(identity))`. |
| `f3` | `scala.concurrent.Future[scala.collection.immutable.IndexedSeq[Boolean]]` | Single future wrapping the failsafe sequence of booleans produced by `Future.sequence(resultsFailsafe)`. |
| `f4` | `scala.concurrent.Future[Int]` | Single future containing the final prime count derived from `f3`. |

---

### Question 3: What role does the `Future.sequence` method play?

**Answer:**
`Future.sequence` is an asynchronous combinator (often referred to as an "inversion of container structure" or traverse/sequence operation). Specifically:
- **Structure Inversion:** It transforms a collection of futures `Seq[Future[T]]` into a single future containing a collection `Future[Seq[T]]`. In this program, it transforms `IndexedSeq[Future[Boolean]]` into `Future[IndexedSeq[Boolean]]`.
- **Concurrent Coordination:** It allows many independent background tasks initiated in parallel to be treated as a single unified asynchronous computation. Instead of attaching callbacks or listeners to hundreds of individual futures, callers can attach a single callback (`f1.map(...)` or `f1.onComplete(...)`) to be triggered once the entire batch completes.
- **Fail-Fast Semantics:** By default, `Future.sequence` is fail-fast. If **any single** constituent future fails with an exception (e.g. `ConnectException`, socket timeout), the resulting combined future immediately fails with that exception, discarding the results of the other completed futures. This is why the code provides `resultsFailsafe`: by attaching `.recover` to each individual future *before* calling `Future.sequence`, any connection failure is converted to `false`, guaranteeing that `f3` and `f4` will successfully complete even when some requests encounter network errors.

---

### Question 4: How many elements does `f2` contain (when viewed as a container, regardless of the specific values contained)?

**Answer:**
When viewed as an effect container (monadic container), a `Future[T]` contains **at most one** element:
- While asynchronous computation is in progress or if the computation fails with an exception, it contains **0 elements** (represented by `None` or `Failure(exception)` in `.value`).
- When successfully completed, it contains **exactly 1 element** (represented by `Some(Success(value))` in `.value`).

In this specific case, `f2` has type `Future[Int]`. Regardless of how many primes were checked or counted, `f2` holds a single scalar integer representing the total count.

---

### Question 5: How do the results of running `c.printProgress()` and `c.printResults()` change over time?

#### Progress Tracking (`c.printProgress()`):
`printProgress()` prints two values:
```scala
println(results.count(f => f.isCompleted))
println(resultsFailsafe.count(f => f.isCompleted))
```
- **At start:** Prints `0` and `0` (or low numbers as the first few network round-trips finish).
- **Over time:** Both numbers increment in lockstep (e.g., `186`, `793`, `989`, `1001`), reflecting the number of HTTP requests that have received a response or failed. They move at the identical rate because `resultsFailsafe` is derived from `results` and completes at the exact same moment.
- **At completion:** Both numbers equal the total number of requests (`end - start + 1`).

#### Results Observation (`c.printResults()`):
`printResults()` inspects the current `.value` of `f2` and `f4`:
```scala
println(f2.value)
println(f4.value)
```
- **While running:** Both `f2.value` and `f4.value` return `None`, because the aggregate futures are awaiting the completion of all child futures.
- **Upon completion:**
  - **Scenario A (Standard network conditions with occasional timeout/error):**
    - `f2.value` prints `Some(Failure(java.net.ConnectException: handshake timed out...))` because unhandled network failure in even one request causes `f1` and `f2` to fail fast.
    - `f4.value` prints `Some(Success(75))` because `resultsFailsafe` intercepted errors via `.recover`, allowing `f4` to complete successfully and deliver the prime count.
  - **Scenario B (100% successful requests):**
    - Both `f2.value` and `f4.value` print `Some(Success(count))`.

---

### Question 6: What else did you find noteworthy or worth wondering about?

1. **Fail-Fast vs. Resilient Fault Tolerance:**
   - The contrast between `f2` (raw) and `f4` (failsafe) illustrates a fundamental pattern in distributed systems. When making hundreds of remote HTTP calls over an unreliable network, failure of a single endpoint is virtually guaranteed at scale. Without per-request error handling (such as `.recover`), an entire batch computation is lost.
2. **HTTP Semantics for Domain Logic:**
   - The microservice maps domain logic to HTTP response codes: `200` for primes and `404` for composites. While concise, using `404 Not Found` for a valid mathematical calculation conflates web infrastructure routing errors (e.g. wrong URL path) with negative domain results.
3. **Throttling and Concurrency Control:**
   - The implementation dispatches all requests simultaneously (`(start to end).map(...)`). For small ranges (1,000 requests), this can easily exhaust local socket pools, trigger operating system file descriptor limits, or hit rate limits / DDoS protection (e.g. Cloudflare or Render 429/503).
   - In production, one would typically use a stream-based approach with bounded concurrency (such as `Source(start to end).mapAsync(parallelism = 16)(...)` using Akka/Pekko Streams) to regulate the rate of outgoing requests.
4. **Completely Non-Blocking Architecture:**
   - The code does not use blocking calls like `Await.result`. Everything runs asynchronously on the implicit global `ExecutionContext` (a ForkJoinPool). The REPL stays responsive while background threads perform I/O and computations.
5. **Cold Starts in Cloud Environments:**
   - Free-tier serverless or container hosts (like Render) spin down during periods of inactivity. The initial wave of requests can trigger connection timeouts before the remote container finishes cold boot.

---

## 3. Summary of Findings & Conclusions
- Scala `Future`s combined with functional combinators like `map`, `recover`, and `Future.sequence` provide a powerful, declarative abstraction for managing concurrent asynchronous workflows.
- Handling partial failures at the individual future level is crucial when aggregating distributed operations to prevent fail-fast cascading errors.
- Background asynchronous tasks decouple I/O latency from the main execution thread, allowing real-time progress monitoring and responsive user interactions.
