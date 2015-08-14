Hello OpenICE
=========

A small example program to demonstrate retrieving data from an [OpenICE](http://mdpnp.sourceforge.net) system.

  - Written in Java
  - Pulls single numeric values
  - Pulls sample array (waveform tracing) data
  - Shows delivery of data via either the application thread or an external middleware thread

Build And Run
--------------

You can provide simulated data to this receiver program by downloading and running our
distribution of OpenICE from [GitHub project releases](https://github.com/mdpnp/mdpnp/releases/latest).
Please ensure that the version of the distribution matches the version referenced in this project's build.gradle
configuration.  If you have questions you can reach the team at [community.openice.info](http://community.openice.info).

```sh
git clone https://www.github.com/mdpnp/hello-openice
cd hello-openice
./gradlew run
```

Ctrl-C to exit the program.


