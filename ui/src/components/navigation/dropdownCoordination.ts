const coordinatedDropdownAttribute = "data-vectis-dropdown";
const coordinatedDropdownSelector = `details[${coordinatedDropdownAttribute}="true"]`;

export const coordinatedDropdownProps = {
  [coordinatedDropdownAttribute]: "true"
};

export function closeParentDropdown(element: HTMLElement) {
  const details = element.closest("details");

  if (details) {
    details.open = false;
  }
}

export function closeOtherDropdowns(currentDetails: HTMLDetailsElement) {
  if (!currentDetails.open) {
    return;
  }

  closeCoordinatedDropdowns(currentDetails.ownerDocument, currentDetails);
}

export function closeOtherDropdownsFromTrigger(element: HTMLElement) {
  const currentDetails = element.closest("details");

  if (currentDetails) {
    closeCoordinatedDropdowns(currentDetails.ownerDocument, currentDetails);
  }
}

export function closeCoordinatedDropdowns(root: ParentNode, exceptDetails?: HTMLDetailsElement) {
  root.querySelectorAll<HTMLDetailsElement>(coordinatedDropdownSelector).forEach((details) => {
    if (details !== exceptDetails) {
      details.open = false;
    }
  });
}

export function isInsideCoordinatedDropdown(target: EventTarget | null) {
  return target instanceof Element && Boolean(target.closest(coordinatedDropdownSelector));
}
