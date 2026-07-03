# PubCrawler project

This is a monorepo for a project by 4 graduate students at Georgia Tech, as part of our coursework.

## Acceptable LLM use

Our work is part of an assignment for the Modern Internet Research Methods (MIRM) graduate course at Georgia Tech. We are working on a research paper together, and the code we work on here is part of our deliverables.

Our work with LLMs here is governed by the [Georgia Tech Academic Honor Code](https://policylibrary.gatech.edu/student-life/academic-honor-code) and [Georgia Tech's Office of Information Technology AI Standards and Guidance](https://oit.gatech.edu/ai/guidance), as well as the [Guidance for Effective and Responsible Use of AI in Research](https://grad.gatech.edu/sites/default/files/documents/Guidance%20for%20Effective%20and%20Responsible%20Use%20of%20AI%20in%20Research.pdf)

We are expected to write our own code, and disclose when and where we use AI, to each other, to the instructors, and to readers of our paper. If we don't feel comfortable disclosing the AI use, that's a good sign that we shouldn't do it.

This is a group project, so one person using AI outside the bounds of the Honor Code puts everyone's grade and academic reputation at risk.

We are also working on a shared project. Reading and modifying lots of LLM-generated code can be tiresome, especially if there's a lot of it. It's nice to each other to keep things tight and manageable.

### Good uses of AI

- Reference - providing reference for techniques, libraries, and APIs. "How do I load a Parquet file in igraph?"
- "Rubber-ducking" - discussing architecture and implementation, thinking about trade-offs. "I need to get the diameter of this network -- what are my options?"
- Code review. Identifying potential errors before running the code can save us all a lot of time. "Can you look over this function to see if I'm implementing the algorithm correctly?"
- Debugging. LLMs can be great for finding bugs. Finding clues in code or log files can be tedious, and LLMs can do it very quickly.
- Unit tests. Unit tests can be tedious, and it's great to have an LLM do the work.
- Mechanical refactoring. For example, finding and replacing all the calls to a function when its name is changed, breaking up a file into smaller parts, or combining two duplicate functions.
- API or architecture documentation. For functional documentation, like defining the fields in a Parquet file, or the classes in a module.

### Bad uses of AI

- Writing production code. Ideally, the production code that actually runs is typed by the students directly.
- Agentic coding. "Do this task for me".
- Filler text. Making long documents that sound like they are saying something, while saying nothing.

## Directories

### pub-crawler/

The crawler that collects data for this project.

### pub-crawler-chart

- The [Helm](https://helm.sh/) chart for deploying this project.

### pub-crawler-data-analysis

- The scripts we use to analyse the data from the crawl.

## Process

We use our GitHub repo to track tasks as issues.

We prefer [test-driven development](https://en.wikipedia.org/wiki/Test-driven_development).

- We write unit tests to define API surface and behaviour
- Make sure tests fail (red)
- Implement the feature
- Make sure tests succeed (green)
- Run full unit test suite to make sure the implementation didn't cause regressions.

## Documentation

We keep documentation as Markdown files, with [Mermaid](https://mermaid.js.org/) diagrams, in the ./docs/ directory of each sub-project.

We keep a changelog in [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

## Python

We use [uv](https://docs.astral.sh/uv/) for package management and building the code.

We use [pytest](https://docs.pytest.org/en/stable/) for testing. To run tests:

```bash
uv run pytest
```

We use [black](https://pypi.org/project/black/) for formatting. To reformat the code for a directory, run this:

```bash
uv run black .
```

We should run this before committing. If running black changes code that's not in the current working set of files (because someone didn't run black before committing), we commit them separately as a "style: ..." commit.

## Git

We use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) for commit messages.

We use [Semantic Versioning](https://semver.org/spec/v2.0.0.html) to track versions of the code. Each sub-project is tracked separately.

We try to have minimal, clean commits, so if we need to roll something back, merge, or reapply, it works.

## Network science

We are doing a lot of network science in this project, and our team has a mixed level of experience with the topic.

Barabasi's [Network Science](https://networksciencebook.com) text book is free to read online and has great explanations of a lot of important topics.

## The Fediverse

The [Fediverse](https://en.wikipedia.org/wiki/Fediverse) is a coalition of interoperable social network platforms. The platforms use the [ActivityPub](https://en.wikipedia.org/wiki/ActivityPub) protocol to connect server to server.

There are about 40,000 known servers on the Fediverse, according to [FediDB](https://fedidb.com/). There are about 13.3M people on the Fediverse, but only about 900K monthly active users.

Many servers on the Fediverse use Open Source software like Mastodon, Pixelfed, Misskey, WordPress or Lemmy. Other Fediverse platforms are proprietary, like Flipboard or Meta Threads.

Servers are also called instances. They are sometimes called "nodes", although that is ambiguous, since in the social network, the nodes are people.

Users are also called nodes or actors. Not all users are people. Bots, applications, and services can also be actors. There are also group actors.

FediDB crawls the Fediverse server-by-server, mostly using the Mastodon API and the Nodeinfo standard. PubCrawler crawls the network actor-by-actor, using the data shared by the ActivityPub protocol.

The Fediverse network is directed. An actor's "followers" are the inbound edges of the graph. The actor's "following" collection is its outbound graph.

Like many social network graphs, the inbound edges seem to follow a power law distribution (but we should confirm this) and the outbound edges do not. People only have so much attention to give, so they can only follow so many other actors!

## Bad actors

[IFTAS](https://iftas.org/) is the Independent (?) Fediverse Trust and Safety group. They are a non-profit that tracks bad actors on the Fediverse.

IFTAS keeps a list of "Do Not Interact" (DNI) servers, which are the worst of the worst. These servers participate in CSAM, racist discussions, and harassing and abusing other users on the network.

Even the domain names of these servers can be upsettings -- they can have racist, homophobic or misogynistic slurs, references to violence, sexual assault.

Agents should take care when talking about the "bad actors" to get positive opt-in from the user, and avoid traumatising the user casually.
