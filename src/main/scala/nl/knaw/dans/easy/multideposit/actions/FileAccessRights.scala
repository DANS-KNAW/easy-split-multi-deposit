package nl.knaw.dans.easy.multideposit.actions

/**
 * Enumeration of the file properties VisibleTo and AccessibleTo
 * Partially copied from easy-stage-file-item
 */
// TODO move this enum to the DDM library
object FileAccessRights extends Enumeration {

  type UserCategory = Value
  val
  ANONYMOUS, // a user that is not logged in
  KNOWN, // a logged in user
  RESTRICTED_REQUEST, // a user that received permission to access the dataset
  RESTRICTED_GROUP, // a user belonging to the same group as the dataset
  NONE // none of the above
  = Value
}
