from operator import attrgetter

class Action:
    """
    This is the Action class. Do not modify this.
    """

    def __init__(self, object_, transaction, type_):
        self.object_ = object_
        self.transaction = transaction
        assert type_ in ("REQUEST_LOCK", "REQUEST_UNLOCK")
        self.type_ = type_

    def __repr__(self):
        return f"Action({self.object_}, {self.transaction}, {self.type_})"

    def __eq__(self, other):
        return self.object_ == other.object_ and self.transaction == other.transaction and self.type_ == other.type_


class WaitsForGraphTracker:
    """
    Please modify this class.
    """

    def __init__(self):
        """
        Constructor.
        """
        self.queued_actions = []
        self.wait_for_graph = {}
        self.locked_objects = {}

    def add_action(self, action):
        """
        Indicates the action that the database is trying to perform (see class above).
        Return the list of actions that are queued to be performed once waiting transactions
        are able to run.
        """
        if action.type_ == "REQUEST_UNLOCK":
            assert action.object_ in self.locked_objects, "{} not in {}".format(action.object_, self.locked_objects)

            # Just unlock it if there isn't another action from the same transaction that's already in the queue
            if action.transaction not in [a.transaction for a in self.queued_actions]:
                self.locked_objects.pop(action.object_, None)

                # List of transactions that were dependent on the transaction that just unlocked
                dependent_transactions = [key for key, val in self.wait_for_graph.items() if val == action.transaction]

                actions_freed = False
                for a in self.queued_actions:
                    if a.object_ == action.object_:     # If there's an action that can now lock the just-released object:
                        actions_freed = True
                        break

                if actions_freed:
                    queue = self.queued_actions
                    self.queued_actions = []
                    self.wait_for_graph = {}
                    for a in queue:
                        self.add_action(a)

            # Otherwise, just add it to the action queue
            else:

                self.queued_actions.append(action)

        # If action needs to wait, put it on the list of queued actions and add it to the wait_for_graph dictionary
        # Ignore duplicate lock requests
        elif action.type_ == "REQUEST_LOCK":

            # Does the action need to wait?
            # YES if (object is locked and it's locked by a different transaction)
            if action.object_ in self.locked_objects and action.transaction != self.locked_objects[action.object_]:

                self.queued_actions.append(action)

                # Figure out which transaction is being waited on
                blocking_transaction = self.locked_objects[action.object_]
                self.wait_for_graph[action.transaction] = blocking_transaction

            # If the object is unlocked but the transaction is blocked, only add the action to the queued_actions,
            # The action is waiting for an earlier action in the transaction to pass
            elif action.object_ not in self.locked_objects and action.transaction in self.wait_for_graph:
                self.queued_actions.append(action)

            # If the action doesn't need to wait, it gets the lock and isn't queued. Add the object to locked_objects
            else:
                self.locked_objects[action.object_] = action.transaction
        return self.queued_actions

    def get_current_graph(self):
        """
        Return a dictionary that represents the waits-for graph of the transactions. The key is
        the name of the transaction that is waiting, and the value the name of the transaction that
        is holding the desired lock.
        """
        return self.wait_for_graph








##################################### TESTING #########################################################

wfgt = WaitsForGraphTracker()
assert({} == wfgt.get_current_graph())

remaining_actions = wfgt.add_action(Action("A", "1", "REQUEST_LOCK"))
assert([] == remaining_actions)
assert({} == wfgt.get_current_graph())

remaining_actions = wfgt.add_action(Action("B", "1", "REQUEST_LOCK"))
assert([] == remaining_actions)
assert({} == wfgt.get_current_graph())

remaining_actions = wfgt.add_action(Action("B", "2", "REQUEST_LOCK"))
assert([
    Action("B", "2", "REQUEST_LOCK"),
    ] == remaining_actions)
assert({'2': '1'} == wfgt.get_current_graph())

remaining_actions = wfgt.add_action(Action("B", "2", "REQUEST_UNLOCK"))
assert([
    Action("B", "2", "REQUEST_LOCK"),
    Action("B", "2", "REQUEST_UNLOCK"),
    ] == remaining_actions)
assert({'2': '1'} == wfgt.get_current_graph())

remaining_actions = wfgt.add_action(Action("C", "2", "REQUEST_LOCK"))
assert([
    Action("B", "2", "REQUEST_LOCK"),
    Action("B", "2", "REQUEST_UNLOCK"),
    Action("C", "2", "REQUEST_LOCK"),
    ] == remaining_actions)
assert({'2': '1'} == wfgt.get_current_graph())

remaining_actions = wfgt.add_action(Action("C", "3", "REQUEST_LOCK"))
assert([
    Action("B", "2", "REQUEST_LOCK"),
    Action("B", "2", "REQUEST_UNLOCK"),
    Action("C", "2", "REQUEST_LOCK"),
    ] == remaining_actions)
assert({'2': '1'} == wfgt.get_current_graph())

remaining_actions = wfgt.add_action(Action("B", "1", "REQUEST_UNLOCK"))
assert([
    Action("C", "2", "REQUEST_LOCK"),
    ] == remaining_actions)
assert({'2': '3'} == wfgt.get_current_graph())

