# MRSort: A distributed string merge-sorting system based on Java and ZeroMQ

[Chinese](./README.md) | English

This program implements a distributed string sorting system as a course project (major assignment) for the Summer 2022 "Mathematics Practice" course. Please check with your subject professor beforehand to determine what approaches you may use if you wish to replicate the work.

The presentation script in Chinese is available at [presentation.md](. /presentation.md).

## Original requirement

- Sort a total data file set of 320G (bytes) to compete for sorting speed.

- The file contains a random alphabetic string of length 15, `/n` line feed (see sample file), sort the strings in character order, again `/n` line feed, and output to the file. The scores were ranked according to correctness and completion time.

- In the same room, 6 teams from each pair of classes will compete. Each team will have access to 8 computers in the lab. The files that need to be sorted are already installed and have the names `data01.txt` through `data08.txt` in the `data` directory of E drive (40G, or 2.5 billion records) for each machine. The necessary software is already installed on the machines (JAVA8 and PYTHON3.7, only these two software options are allowed).

- Beginning at the same moment, each group may run each computer, load its own software, and begin sorting jobs. The output should be placed in a new group name directory in the root directory of the D drive (e.g. class 1, group 1, machine 1 resulted directory named `c1g1m1`), and the output should be divided into 26 files according to the first letter of the string and placed in order on 8 machines (file name `resulta.txt` - `resultz.txt`, the first 8 files in order on machines 1 - 2, the last 18 files in order on machines 3 - 8). When done, snap a screenshot (containing the directory name, file name, file size, and time of creation) and send it as a jpeg image to the QQ group.

- Two people are chosen at random from each group to defend the group, describe the implementation method, plan, the code of the project, and answer questions from the teacher. The teacher will assign grades in accordance with this.

## Requirements confirmed after communication

- You can bring your own JRE
- Can call external libraries

## What was not covered in the presentation

- I considered using GraalVM to compile this program into native code (while still ensuring the code is still validate Java 8 code), but eventually gave up on this idea because I couldn't figure out how to use the plug-in in Gradle.
- Oracle Java SE 8 was installed on lab workstations, and I brought Azul Zulu 17, supposedly the fastest JRE in the past. (But I still use `project.targetCompatibility` to make sure my code is compatible Java 8)
  - As the main reason for switching to new version, I wanted to try ZGC, but I found that OOM would be easily triggered, so I ended up with G1 GC.
  - Java 8 is a bit annoying to write (e.g. the `Optional<>` lack of `or`), but far from having impact on task completing.
- For performance profiling, I used [System Informer](https://github.com/winsiderss/systeminformer) (when its name was still Process Hacker) and [VisualVM](https://visualvm.github.io/).
- [ImHex](https://github.com/WerWolv/ImHex) was used for binary viewing.
- The C drive of workstations are SSD, so I decided to place the cache there.
- On D-day
  - The IP address of workstations changed, causing a large portion of the data to be lost at the start.
  - The result file size was evidently too tiny, but I dismissed it at first (since We was the first group to finish despite the above-mentioned difficulty wasting time) and went out straight to instruct other group members on presenting.
  - When I returned to the lab and examined the data, I discovered that there was an issue with the merging procedure. So I built a program that re-merged the files based on meta-data revealed from their file names, and the final output file size was double what was intended, with a huge number of adjacent two lines repeated.
  - It was now found that the fault in the merging process was caused by file deletion. I intended to develop a program to directly reduplicate the output file, but I couldn't decide whether to remove one line directly from two lines or to judge deduplication, and eventually picked the former, but it didn't run out before the final time limit due to the speed of the hard drive.
  - The larger file was chosen as the final version of the results submitted.
