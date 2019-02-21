=========
Changelog
=========

All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <https://keepachangelog.com/en/1.0.0/>`_,
and this project adheres to `Semantic Versioning <https://semver.org/spec/v2.0.0.html>`_.

[Unreleased]
============

Added
*****
* Implemented /queries enpoints. Specifically:

  ``/queries``
    Returns a list of all the queries currently in the database.

  ``/queries/{id}``
    Returns a description of the query with the given ``{id}``.

  ``/queries/{id}/executions``
    Returns a list of executions associated with the given ``{id}``.
